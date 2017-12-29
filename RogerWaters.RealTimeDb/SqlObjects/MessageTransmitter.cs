using System;
using System.Threading;
using System.Xml.Linq;
using RogerWaters.RealTimeDb.Configuration;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// Reads messages from configured queue and broadcast them to any Listener
    /// </summary>
    internal sealed class MessageTransmitter:IDisposable
    {
        /// <summary>
        /// The database the reader operates in
        /// </summary>
        private readonly Database _db;

        /// <summary>
        /// The conversation handle events are transmitted through
        /// </summary>
        public Guid ConversationHandle { get; }
        
        /// <summary>
        /// Thread that reads events and target them to the expected class
        /// </summary>
        private readonly Thread _messageReader;

        /// <summary>
        /// Source to cancel <see cref="_messageReader"/>
        /// </summary>
        private readonly CancellationTokenSource _cancellationTokenSource;

        /// <summary>
        /// Occurred any time a Message is received
        /// </summary>
        public event Action<XElement> MessageRecieved;

        /// <summary>
        /// Initialize a new instance of <see cref="MessageTransmitter"/>
        /// </summary>
        /// <param name="db"></param>
        public MessageTransmitter(Database db)
        {
            _db = db;
            DatabaseConfig config = db.Config;
            Guid conversation = Guid.Empty;

            config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.CreateMessageType(config.MessageTypeName);
                    transaction.CreateContract(config.ContractName, config.MessageTypeName);
                    transaction.CreateQueue(config.SenderQueueName);
                    transaction.CreateQueue(config.ReceiverQueueName);
                    transaction.CreateService(config.SenderServiceName, config.SenderQueueName, config.ContractName);
                    transaction.CreateService(config.ReceiverServiceName, config.ReceiverQueueName, config.ContractName);
                    conversation = transaction.GetConversation(config.SenderServiceName, config.ReceiverServiceName, config.ContractName);
                    transaction.Commit();
                }
            });
            ConversationHandle = conversation;
            _messageReader = new Thread(ProcessMessages);
            _cancellationTokenSource = new CancellationTokenSource();
            _messageReader.Start();
        }
        
        /// <summary>
        /// The internal listener that receive messages
        /// </summary>
        private void ProcessMessages()
        {
            var token = _cancellationTokenSource.Token;
            while (token.IsCancellationRequested == false)
            {
                foreach (var message in _db.Config.DatabaseConnectionString.ReceiveMessages(_db.Config.ReceiverQueueName, TimeSpan.FromSeconds(5)))
                {
                    if (token.IsCancellationRequested)
                    {
                        return;
                    }
                    var root = XElement.Parse(message);
                    MessageRecieved?.Invoke(root);
                }
            }
        }

        /// <summary>
        /// Stops receiving and emitting events and cleanup all associated db objects
        /// </summary>
        public void Dispose()
        {
            if ((_messageReader.ThreadState & ThreadState.Unstarted) == 0)
            {
                _cancellationTokenSource.Cancel();
                _messageReader.Join();
            }

            var config = _db.Config;
            config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.EndConversation(ConversationHandle);
                    transaction.DropService(config.SenderServiceName);
                    transaction.DropService(config.ReceiverServiceName);
                    transaction.DropQueue(config.SenderQueueName);
                    transaction.DropQueue(config.ReceiverQueueName);
                    transaction.DropContract(config.ContractName);
                    transaction.DropMessageType(config.MessageTypeName);
                    transaction.Commit();
                }
            });
        }
    }
}
