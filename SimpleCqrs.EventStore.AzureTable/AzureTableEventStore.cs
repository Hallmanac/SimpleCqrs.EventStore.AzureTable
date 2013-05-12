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
        private readonly string _accountKey;
        private readonly string _accountName;
        private readonly DomainEvent _emptyDomainEvent = new DomainEvent();
        private List<string> _aggregateRootIdsInTable = new List<string>();
        private PartitionSchema<DomainEvent> _latestVersionPartition; 

        private CloudTableContext<DomainEvent> _domainEventContext;

        public AzureTableEventStore(string accountKey, string accountName)
        {
            _accountKey = accountKey;
            _accountName = accountName;
            var storageCreds = new StorageCredentials(_accountName, _accountKey);
            var storageAccount = new CloudStorageAccount(storageCreds, true);
            Init(storageAccount);
        }

        public AzureTableEventStore(CloudStorageAccount cloudStorageAccount) { Init(cloudStorageAccount); }

        #region IEventStore Members
        public IEnumerable<DomainEvent> GetEvents(Guid aggregateRootId = default(Guid), int startSequence = 0)
        {
            // If we don't get an ID passed into us then that indicates that we want to play back the entire event stream
            if(aggregateRootId == default(Guid))
            {
                return _aggregateRootIdsInTable.SelectMany(arIdString => _domainEventContext.GetByPartitionKey(arIdString));
            }
            return _domainEventContext.GetByPartitionKey(aggregateRootId);
        }

        public void Insert(IEnumerable<DomainEvent> domainEvents)
        {
            List<DomainEvent> domainEventsList = domainEvents as List<DomainEvent> ?? domainEvents.ToList();
            VerifyPartitionsExist(domainEventsList);
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
            foreach(DomainEvent domainEvent in domainEventsList)
            {
                string arRootString = domainEvent.AggregateRootId.SerializeToString();
                int latestVersionValue = 0;
                if(!(aggregatesChecked.ContainsKey(arRootString)))
                {
                    DomainEvent latestVersionInstance = _domainEventContext.GetById(arRootString, _latestVersionPartition.PartitionKey);
                    if(latestVersionInstance != null) latestVersionValue = latestVersionInstance.Sequence;
                    aggregatesChecked.Add(arRootString, latestVersionValue);
                }
                if(aggregatesChecked[arRootString] > domainEvent.Sequence) 
                    throw new EventStoreConcurrencyException(aggregatesChecked[arRootString], domainEvent, domainEventsList);
            }
        }

        private void VerifyPartitionsExist(List<DomainEvent> domainEventsList)
        {
            var newPartitionSchemas = new List<PartitionSchema<DomainEvent>>();
            foreach(DomainEvent domainEvent in domainEventsList)
            {
                string serializedAggregateRootId = domainEvent.AggregateRootId.SerializeToString();
                // Check if the Aggregate Root ID is in the list of id strings
                if(_aggregateRootIdsInTable.All(rootId => rootId != serializedAggregateRootId)) 
                    _aggregateRootIdsInTable.Add(serializedAggregateRootId);
                
                // Check if there is a partition for the given domain event type followed by one for the AggregateRootId
                if(_domainEventContext.PartitionKeysInTable.All(pk => pk != domainEvent.GetType().Name))
                {
                    PartitionSchema<DomainEvent> eventTypeInstanceSchema = _domainEventContext.CreatePartitionSchema(domainEvent.GetType().Name)
                        .SetRowKeyCriteria(domainEvt => _domainEventContext.GetChronologicalBasedRowKey())
                        .SetSchemaCriteria(domainEvt => domainEvt.AggregateRootId.SerializeToString() == serializedAggregateRootId)
                        .SetIndexedPropertyCriteria(domainEvt => domainEvt.AggregateRootId.SerializeToString());
                    newPartitionSchemas.Add(eventTypeInstanceSchema);
                    
                    if(_domainEventContext.PartitionKeysInTable.All(pk => pk != serializedAggregateRootId))
                    {
                        PartitionSchema<DomainEvent> domainEventIdSchema = _domainEventContext.CreatePartitionSchema(serializedAggregateRootId)
                            .SetRowKeyCriteria(domainEvt => _domainEventContext.GetChronologicalBasedRowKey())
                            .SetSchemaCriteria(domainEvt => domainEvt.AggregateRootId.SerializeToString() == serializedAggregateRootId)
                            .SetIndexedPropertyCriteria(domainEvt => domainEvt.GetType().Name);
                        newPartitionSchemas.Add(domainEventIdSchema);
                    }
                }
                    // Check to see if there is a partition key for the given AggregateRootId.
                else if(_domainEventContext.PartitionKeysInTable.All(pk => pk != serializedAggregateRootId))
                {
                    PartitionSchema<DomainEvent> domainEventIdSchema = _domainEventContext.CreatePartitionSchema(serializedAggregateRootId)
                        .SetRowKeyCriteria(domainEvt => _domainEventContext.GetChronologicalBasedRowKey())
                        .SetSchemaCriteria(domainEvt => domainEvt.AggregateRootId.SerializeToString() == serializedAggregateRootId)
                        .SetIndexedPropertyCriteria(domainEvt => domainEvt.GetType().Name);
                    newPartitionSchemas.Add(domainEventIdSchema);
                }
            }
            _domainEventContext.AddMultiplePartitionSchemas(newPartitionSchemas);
        }

        private void Init(CloudStorageAccount storageAccount)
        {
            _domainEventContext = new CloudTableContext<DomainEvent>(storageAccount, this.GetPropertyName(() =>_emptyDomainEvent.AggregateRootId));
            
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