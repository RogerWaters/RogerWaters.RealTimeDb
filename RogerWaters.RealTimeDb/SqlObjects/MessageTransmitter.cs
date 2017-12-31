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
        /// The configuration from the database
        /// </summary>
        private readonly DatabaseConfig _config;

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
            _config = db.Config;
            Guid conversation = Guid.Empty;

            _config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.CreateMessageType(_config.MessageTypeName);
                    transaction.CreateContract(_config.ContractName, _config.MessageTypeName);
                    transaction.CreateQueue(_config.SenderQueueName);
                    transaction.CreateQueue(_config.ReceiverQueueName);
                    transaction.CreateService(_config.SenderServiceName, _config.SenderQueueName, _config.ContractName);
                    transaction.CreateService(_config.ReceiverServiceName, _config.ReceiverQueueName, _config.ContractName);
                    conversation = transaction.GetConversation(_config.SenderServiceName, _config.ReceiverServiceName, _config.ContractName);
                    transaction.Commit();
                }
            });
            ConversationHandle = conversation;
            _messageReader = new Thread(ProcessMessages){ IsBackground = true };
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
                foreach (var message in _config.DatabaseConnectionString.ReceiveMessages(_config.ReceiverQueueName, TimeSpan.FromSeconds(5)))
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
                if (Environment.HasShutdownStarted)
                {
                    _messageReader.Abort();
                }
                else
                {
                    _messageReader.Join();
                }
            }
            
            _config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.EndConversation(ConversationHandle);
                    transaction.DropService(_config.SenderServiceName);
                    transaction.DropService(_config.ReceiverServiceName);
                    transaction.DropQueue(_config.SenderQueueName);
                    transaction.DropQueue(_config.ReceiverQueueName);
                    transaction.DropContract(_config.ContractName);
                    transaction.DropMessageType(_config.MessageTypeName);
                    transaction.Commit();
                }
            });
        }
    }
}
