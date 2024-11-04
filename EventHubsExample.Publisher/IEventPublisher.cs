namespace EventHubsExample.Publisher;

public interface IEventPublisher
{
    Task SendEventAsync(string message);
    
    /// <summary>
    /// Belirli bir sınıra göre gönderim.
    /// </summary>
    /// <param name="messages"></param>
    /// <returns></returns>
    Task SendEventsInBatchesAsync();

    Task SendEventsWithRequestCountAsync(List<string> messages, int batchSize);


    /// <summary>
    /// ConcurrentQueue ve zamanlayıcı ile kontrol edilerek hubs'a gönderim.
    /// </summary>
    /// <returns></returns>
    Task RequestWithPublisher();



    #region Retry Mekanizmaları

    Task SendEventWithRetryAsync(string message);

    #endregion
}