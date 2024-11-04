using System.Text;
using Azure.Messaging.EventHubs.Consumer;

namespace EventHubsExample.Consumer;

public class EventConsumer: IEventConsumer
{
    private const string connectionString = "<EVENT_HUBS_CONNECTION_STRING>";
    private const string eventHubName = "<EVENT_HUB_NAME>";
    private const string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;
    
    public async Task ReadEventsAsync()
    {
        await using var concumerClient=  new EventHubConsumerClient(consumerGroup, connectionString, eventHubName);

        using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(30));

        await foreach (PartitionEvent partitionEvent in concumerClient.ReadEventsAsync(cancellationTokenSource.Token))
        {
            string message= Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray());
        }
    }
    
    // MassTaransit'i direk olrak message işlemek için kullanamayız. Fakat Hubs'dan alınan mesajları IMessage olarak işleyebilir.
    // Ve ardından başka bir kuyruğa veya servize yönlendirebilir. MassTransit bir Event Hub tüketicisi olarak değil de
    // okunan Event Hubs mesajlarını diğer mesajlaşma araçlarına yönlendirebilir.
}