namespace EventHubsExample.Consumer;

public interface IEventConsumer
{
    Task ReadEventsAsync();
}