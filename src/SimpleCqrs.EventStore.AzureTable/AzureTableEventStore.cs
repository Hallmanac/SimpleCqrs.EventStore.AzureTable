namespace SimpleCqrs.EventStore.AzureTable
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using AzureCloudTableContext.Api;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using ServiceStack.Text;
    using SimpleCqrs.Eventing;

    public class AzureTableEventStore : IEventStore
    {
        private const string AggregateRootIdPropName = "AggregateRootId";
        private readonly List<string> _aggregateRootIdsInTable = new List<string>();
        private readonly DomainEvent _emptyDomainEvent = new DomainEvent();

        private CloudTableContext<DomainEvent> _domainEventContext;
        private PartitionSchema<DomainEvent> _latestVersionPartition;

        public AzureTableEventStore(string accountKey, string accountName)
        {
            var storageCreds = new StorageCredentials(accountName, accountKey);
            var storageAccount = new CloudStorageAccount(storageCreds, true);
            Init(storageAccount);
        }

        public AzureTableEventStore(CloudStorageAccount cloudStorageAccount) { Init(cloudStorageAccount); }

        #region IEventStore Members
        public IEnumerable<DomainEvent> GetEvents(Guid aggregateRootId = default(Guid), int startSequence = 0)
        {
            // If we don't get an ID passed into us then that indicates that we want to play back the entire event stream
            // but we're not going to do that here since there's a strong possibility that it would crash any given server
            // due to the fact that there might be millions of records and the server might run out of memory. With SimpleCqrs
            // we would need to implement another mechanism that deals with replaying all events to rebuild the read models.
            if(aggregateRootId == default(Guid))
            {
                // Old way --> return _aggregateRootIdsInTable.SelectMany(arIdString => _domainEventContext.GetByPartitionKey(arIdString));
                return new List<DomainEvent>();
            }
            return _domainEventContext.GetByPartitionKey(aggregateRootId);
        }

        public void Insert(IEnumerable<DomainEvent> domainEvents)
        {
            var domainEventsList = domainEvents as List<DomainEvent> ?? domainEvents.ToList();
            VerifyAggregateRootPartitionsExist(domainEventsList);
            CheckAgainstLatestSequence(domainEventsList);
            _domainEventContext.InsertOrReplace(domainEventsList.ToArray());
        }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes)
        {
            return domainEventTypes.ToList().SelectMany(evtType => _domainEventContext.GetByPartitionKey(evtType.Name));
        }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, Guid aggregateRootId)
        {
            var rootIdString = aggregateRootId.SerializeToString();
            return domainEventTypes.ToList().SelectMany(evtType => _domainEventContext.GetByIndexedProperty(rootIdString, evtType.Name));
        }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, DateTime startDate, DateTime endDate)
        {
            var domainEvtsList = new List<DomainEvent>();
            GetEventsByEventTypes(domainEventTypes).ToList().ForEach(domainEvt =>
            {
                if(domainEvt.EventDate >= startDate && domainEvt.EventDate <= endDate)
                {
                    domainEvtsList.Add(domainEvt);
                }
            });
            return domainEvtsList;
        }
        #endregion

        private void CheckAgainstLatestSequence(List<DomainEvent> domainEventsList)
        {
            var aggregatesChecked = new Dictionary<string, int>();
            foreach(var domainEvent in domainEventsList)
            {
                var arRootString = domainEvent.AggregateRootId.SerializeToString();
                var latestVersionValue = 0;
                if(!(aggregatesChecked.ContainsKey(arRootString)))
                {
                    DomainEvent latestVersionInstance;
                    try
                    {
                        latestVersionInstance = _domainEventContext.GetById(arRootString, _latestVersionPartition.PartitionKey);
                    }
                    catch(Exception)
                    {
                        latestVersionInstance = null;
                    }
                    if(latestVersionInstance != null) latestVersionValue = latestVersionInstance.Sequence;
                    aggregatesChecked.Add(arRootString, latestVersionValue);
                }
                if(aggregatesChecked[arRootString] > domainEvent.Sequence) throw new EventStoreConcurrencyException(aggregatesChecked[arRootString], domainEvent, domainEventsList);
            }
        }

        private void VerifyAggregateRootPartitionsExist(IEnumerable<DomainEvent> domainEventsList)
        {
            var newPartitionSchemas = new List<PartitionSchema<DomainEvent>>();
            foreach(var domainEvent in domainEventsList)
            {
                var serializedAggregateRootId = domainEvent.AggregateRootId.SerializeToString();
                // Check if the Aggregate Root ID is in the list of id strings
                if(_aggregateRootIdsInTable.All(rootId => rootId != serializedAggregateRootId)) _aggregateRootIdsInTable.Add(serializedAggregateRootId);
                // Check if there is a partition for the given domain event type followed by one for the AggregateRootId
                if(_domainEventContext.PartitionKeysInTable.All(pk => pk != domainEvent.GetType().Name))
                {
                    var eventTypeInstanceSchema = _domainEventContext.CreatePartitionSchema(domainEvent.GetType().Name)
                                                                     .SetRowKeyCriteria(
                                                                         domainEvt =>
                                                                             _domainEventContext.GetChronologicalBasedRowKey())
                                                                     .SetSchemaCriteria(
                                                                         domainEvt =>
                                                                             domainEvt.AggregateRootId.SerializeToString() ==
                                                                             serializedAggregateRootId)
                                                                     .SetIndexedPropertyCriteria(
                                                                         domainEvt =>
                                                                             domainEvt.AggregateRootId.SerializeToString());
                    newPartitionSchemas.Add(eventTypeInstanceSchema);
                    if(_domainEventContext.PartitionKeysInTable.All(pk => pk != serializedAggregateRootId))
                    {
                        var domainEventIdSchema = _domainEventContext.CreatePartitionSchema(serializedAggregateRootId)
                                                                     .SetRowKeyCriteria(
                                                                         domainEvt =>
                                                                             _domainEventContext.GetChronologicalBasedRowKey())
                                                                     .SetSchemaCriteria(
                                                                         domainEvt =>
                                                                             domainEvt.AggregateRootId.SerializeToString() ==
                                                                             serializedAggregateRootId)
                                                                     .SetIndexedPropertyCriteria(
                                                                         domainEvt => domainEvt.GetType().Name);
                        newPartitionSchemas.Add(domainEventIdSchema);
                    }
                }
                    // Check to see if there is a partition key for the given AggregateRootId.
                else if(_domainEventContext.PartitionKeysInTable.All(pk => pk != serializedAggregateRootId))
                {
                    var domainEventIdSchema = _domainEventContext.CreatePartitionSchema(serializedAggregateRootId)
                                                                 .SetRowKeyCriteria(
                                                                     domainEvt =>
                                                                         _domainEventContext.GetChronologicalBasedRowKey())
                                                                 .SetSchemaCriteria(
                                                                     domainEvt =>
                                                                         domainEvt.AggregateRootId.SerializeToString() ==
                                                                         serializedAggregateRootId)
                                                                 .SetIndexedPropertyCriteria(
                                                                     domainEvt => domainEvt.GetType().Name);
                    newPartitionSchemas.Add(domainEventIdSchema);
                }
            }
            _domainEventContext.AddMultiplePartitionSchemas(newPartitionSchemas);
        }

        private void Init(CloudStorageAccount storageAccount)
        {
            _domainEventContext = new CloudTableContext<DomainEvent>(storageAccount, AggregateRootIdPropName);
            _latestVersionPartition = _domainEventContext.CreatePartitionSchema("LatestVersionPartition")
                                                         .SetRowKeyCriteria(domainEvt => domainEvt.AggregateRootId.SerializeToString())
                                                         .SetSchemaCriteria(domainEvt => true)
                                                         .SetIndexedPropertyCriteria(domainEvt => domainEvt.Sequence);
            _domainEventContext.AddPartitionSchema(_latestVersionPartition);
        }
    }

    public class EventStoreConcurrencyException : Exception
    {
        public EventStoreConcurrencyException(int latestVersion, DomainEvent domainEvent, List<DomainEvent> domainEventsList)
        {
            LatestVersion = latestVersion;
            DomainEventInstance = domainEvent;
            DomainEventsList = domainEventsList;
        }

        public int LatestVersion { get; set; }
        public DomainEvent DomainEventInstance { get; set; }
        public List<DomainEvent> DomainEventsList { get; set; }
    }
}