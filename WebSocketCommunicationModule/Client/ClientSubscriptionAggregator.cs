using Jtext103.CFET2.Core.Communication;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed partial class ClientSubscriptionAggregator
    {
        private readonly Dictionary<ClientSubscriptionGroupKey, ClientSubscriptionGroup> clientSubscriptionGroups = new();
        private readonly Dictionary<Guid, LocalSubscriptionBinding> localSubscriptionIndex = new();
        private readonly object syncRoot = new();

        public ClientSubscriptionPlan Subscribe(Token token, EventFilter filter)
        {
            if (token == null)
            {
                throw new ArgumentNullException(nameof(token));
            }

            if (filter == null)
            {
                throw new ArgumentNullException(nameof(filter));
            }

            if (string.IsNullOrWhiteSpace(filter.Host))
            {
                throw new ArgumentException("Remote event filter host is required.", nameof(filter));
            }

            var endpoint = RemoteEndpointKey.Parse(filter.Host);
            var groupKeys = CreateDistinctGroupKeys(endpoint, filter).ToArray();
            if (groupKeys.Length == 0)
            {
                throw new ArgumentException("Remote event filter must include at least one source and event type.", nameof(filter));
            }

            lock (syncRoot)
            {
                var actions = new List<ClientRemoteAction>();
                var newKeySet = groupKeys.ToHashSet();
                localSubscriptionIndex.TryGetValue(token.Id, out var oldBinding);

                if (oldBinding != null)
                {
                    RemoveLocalTokenFromGroups(
                        token.Id,
                        oldBinding.GroupKeys.Where(key => !newKeySet.Contains(key)),
                        actions);
                }

                var masterTokenIds = new List<Guid>(groupKeys.Length);
                foreach (var groupKey in groupKeys)
                {
                    var group = GetOrCreateClientSubscriptionGroup(groupKey, filter, actions);
                    AddOrUpdateLocalSubscription(group, token.Id, filter.PerformanceLevel, actions);
                    masterTokenIds.Add(group.MasterTokenId);
                }

                localSubscriptionIndex[token.Id] = new LocalSubscriptionBinding
                {
                    LocalTokenId = token.Id,
                    Endpoint = endpoint,
                    GroupKeys = groupKeys,
                    MasterTokenIds = masterTokenIds.ToArray(),
                };

                return new ClientSubscriptionPlan
                {
                    Actions = actions
                };
            }
        }

        public ClientSubscriptionPlan Unsubscribe(Token token)
        {
            if (token == null)
            {
                return new ClientSubscriptionPlan();
            }

            lock (syncRoot)
            {
                if (!localSubscriptionIndex.Remove(token.Id, out var binding))
                {
                    return new ClientSubscriptionPlan();
                }

                var actions = new List<ClientRemoteAction>();
                RemoveLocalTokenFromGroups(token.Id, binding.GroupKeys, actions);
                return new ClientSubscriptionPlan
                {
                    Actions = actions,
                };
            }
        }

        public bool HasLocalSubscriptions(RemoteEndpointKey endpoint)
        {
            lock (syncRoot)
            {
                return clientSubscriptionGroups.Values.Any(group => group.Endpoint.Equals(endpoint) && !group.LocalTokenIds.IsEmpty);
            }
        }

        public void Clear()
        {
            lock (syncRoot)
            {
                clientSubscriptionGroups.Clear();
                localSubscriptionIndex.Clear();
            }
        }

        private ClientSubscriptionGroup GetOrCreateClientSubscriptionGroup(
            ClientSubscriptionGroupKey groupKey,
            EventFilter sourceFilter,
            ICollection<ClientRemoteAction> actions)
        {
            if (clientSubscriptionGroups.TryGetValue(groupKey, out var existingGroup))
            {
                return existingGroup;
            }

            var masterTokenId = Guid.NewGuid();
            var remoteFilter = CreateSingleRuleRemoteFilter(sourceFilter, groupKey);
            var group = new ClientSubscriptionGroup
            {
                MasterTokenId = masterTokenId,
                Key = groupKey,
                RemoteFilter = remoteFilter,
                EffectivePerformanceLevel = sourceFilter.PerformanceLevel,
                CreatedAtUtc = DateTime.UtcNow,
            };

            clientSubscriptionGroups[groupKey] = group;
            actions.Add(new ClientRemoteAction
            {
                Kind = RemoteEndpointActionKind.Subscribe,
                Endpoint = groupKey.Endpoint,
                MasterTokenId = masterTokenId,
                Revision = group.Revision,
                RemoteFilter = remoteFilter,
            });

            return group;
        }

        private void RemoveLocalTokenFromGroups(
            Guid localTokenId,
            IEnumerable<ClientSubscriptionGroupKey> groupKeys,
            ICollection<ClientRemoteAction> actions)
        {
            foreach (var groupKey in groupKeys.ToArray())
            {
                if (!clientSubscriptionGroups.TryGetValue(groupKey, out var group))
                {
                    continue;
                }

                var oldEffectivePerformanceLevel = group.EffectivePerformanceLevel;
                group.LocalTokenIds.TryRemove(localTokenId, out _);
                if (!group.LocalTokenIds.IsEmpty)
                {
                    UpdateEffectivePerformanceLevelIfNeeded(group, oldEffectivePerformanceLevel, actions);
                    continue;
                }

                clientSubscriptionGroups.Remove(groupKey);
                actions.Add(new ClientRemoteAction
                {
                    Kind = RemoteEndpointActionKind.Unsubscribe,
                    Endpoint = group.Endpoint,
                    MasterTokenId = group.MasterTokenId,
                    Revision = group.Revision,
                });
            }
        }

        private static void AddOrUpdateLocalSubscription(
            ClientSubscriptionGroup group,
            Guid localTokenId,
            int performanceLevel,
            ICollection<ClientRemoteAction> actions)
        {
            var oldEffectivePerformanceLevel = group.EffectivePerformanceLevel;
            group.LocalTokenIds[localTokenId] = performanceLevel;
            UpdateEffectivePerformanceLevelIfNeeded(group, oldEffectivePerformanceLevel, actions);
        }

        private static void UpdateEffectivePerformanceLevelIfNeeded(
            ClientSubscriptionGroup group,
            int oldEffectivePerformanceLevel,
            ICollection<ClientRemoteAction> actions)
        {
            if (group.LocalTokenIds.IsEmpty)
            {
                return;
            }

            var newEffectivePerformanceLevel = group.LocalTokenIds.Values.Min();
            if (newEffectivePerformanceLevel == oldEffectivePerformanceLevel)
            {
                return;
            }

            group.EffectivePerformanceLevel = newEffectivePerformanceLevel;
            group.Revision++;
            group.RemoteFilter = CreateSingleRuleRemoteFilter(
                group.RemoteFilter.Host,
                group.Key,
                newEffectivePerformanceLevel);
            actions.Add(new ClientRemoteAction
            {
                Kind = RemoteEndpointActionKind.Subscribe,
                Endpoint = group.Endpoint,
                MasterTokenId = group.MasterTokenId,
                Revision = group.Revision,
                RemoteFilter = group.RemoteFilter,
            });
        }

        private static IEnumerable<ClientSubscriptionGroupKey> CreateDistinctGroupKeys(
            RemoteEndpointKey endpoint,
            EventFilter filter)
        {
            return filter.SourcesAndTypes
                .Where(rule => !string.IsNullOrWhiteSpace(rule.Source) && !string.IsNullOrWhiteSpace(rule.EventType))
                .GroupBy(rule => $"{rule.Source}\0{rule.EventType}", StringComparer.OrdinalIgnoreCase)
                .Select(group =>
                {
                    var rule = group.First();
                    return new ClientSubscriptionGroupKey(endpoint, rule.Source, rule.EventType);
                });
        }

        private static EventFilter CreateSingleRuleRemoteFilter(
            EventFilter sourceFilter,
            ClientSubscriptionGroupKey groupKey)
        {
            return CreateSingleRuleRemoteFilter(
                sourceFilter.Host,
                groupKey,
                sourceFilter.PerformanceLevel);
        }

        private static EventFilter CreateSingleRuleRemoteFilter(
            string host,
            ClientSubscriptionGroupKey groupKey,
            int performanceLevel)
        {
            var remoteFilter = new EventFilter
            {
                Host = host,
                PerformanceLevel = performanceLevel,
            };
            remoteFilter.AddSourceAndType(groupKey.Source, groupKey.EventType);
            return remoteFilter;
        }
    }
}
