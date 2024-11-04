using System.Collections.Concurrent;
using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace EventHubsExample.Publisher;

public class EventPublisher : IEventPublisher
{
    private const string connectionString = "<EVENT_HUBS_CONNECTION_STRING>";
    private const string eventHubName = "<EVENT_HUB_NAME>";
    
    private static readonly ConcurrentQueue<string> requestQueue = new ConcurrentQueue<string>();
    private const int batchSize = 100; // Gönderilecek batch boyutu

    public async Task SendEventAsync(string message)
    {
        await using var producerClient = new EventHubProducerClient(connectionString, eventHubName);
        using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

        if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(message))))
            throw new Exception("Event is too large for the batch.");

        await producerClient.SendAsync(eventBatch);
    }

    public async Task SendEventsInBatchesAsync()
    {
        // Örnek veri listesi
        var messages = new List<string>
        {
            "Message 1",
            "Message 2",
            "Message 3",
            "Message 4",
            // Bu liste, büyük bir veri seti veya çok sayıda istek içerebilir.
        };

        await using var producerClient = new EventHubProducerClient(connectionString, eventHubName);

        // Yeni bir batch oluştur
        var eventBatch = await producerClient.CreateBatchAsync();

        foreach (var message in messages)
        {
            var eventData = new EventData(Encoding.UTF8.GetBytes(message));

            // Eğer event, mevcut batch'e sığmıyorsa mevcut batch'i gönder ve yeni bir batch başlat
            if (!eventBatch.TryAdd(eventData))
            {
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine("Batch gönderildi.");

                // Yeni batch oluştur
                eventBatch = await producerClient.CreateBatchAsync();

                // İlk event'i yeni batch'e ekle
                if (!eventBatch.TryAdd(eventData))
                {
                    throw new Exception("Mesaj batch'e sığmıyor.");
                }
            }
        }

        // Son kalan batch'i gönder
        if (eventBatch.Count > 0)
        {
            await producerClient.SendAsync(eventBatch);
            Console.WriteLine("Son batch gönderildi.");
        }
    }

    public async Task SendEventsWithRequestCountAsync(List<string> messages, int batchSize)
    {
        await using var producerClient = new EventHubProducerClient(connectionString, eventHubName);

        var eventBatch = await producerClient.CreateBatchAsync();
        int currentCount = 0;

        foreach (var message in messages)
        {
            var eventData = new EventData(Encoding.UTF8.GetBytes(message));

            if (!eventBatch.TryAdd(eventData) || currentCount >= batchSize)
            {
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine("Batch gönderildi.");

                eventBatch = await producerClient.CreateBatchAsync();
                currentCount = 0;
                eventBatch.TryAdd(eventData);
            }

            currentCount++;
        }

        if (eventBatch.Count > 0)
        {
            await producerClient.SendAsync(eventBatch);
            Console.WriteLine("Son batch gönderildi.");
        }
    }

    public async Task RequestWithPublisher()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(TimeSpan.FromMinutes(1)); // 1 dakika sonra iptal et

        // Gönderim döngüsünü başlat
        _ = Task.Run(() => ProcessQueueAsync(cancellationTokenSource.Token));

        // Örnek API istekleri kuyruğa ekleniyor
        for (int i = 0; i < 5000; i++)
        {
            AddRequestToQueue($"Request {i}");
        }

        Console.WriteLine("İstekler kuyruğa alındı.");
        await Task.Delay(TimeSpan.FromSeconds(5)); // Kuyruk işlemesi için bekleme

        cancellationTokenSource.Cancel(); // Manuel iptal
        Console.WriteLine("Gönderim işlemi sonlandırıldı.");
    }

    private static void AddRequestToQueue(string request)
    {
        requestQueue.Enqueue(request);
    }

    private async Task ProcessQueueAsync(CancellationToken cancellationToken)
    {
        await using var producerClient = new EventHubProducerClient(connectionString, eventHubName);

        while (!cancellationToken.IsCancellationRequested)
        {
            if (requestQueue.Count >= batchSize)
            {
                var eventBatch = await producerClient.CreateBatchAsync();

                for (int i = 0; i < batchSize; i++)
                {
                    if (!requestQueue.TryDequeue(out var request)) continue;
                    
                    var eventData = new EventData(Encoding.UTF8.GetBytes(request));

                    if (eventBatch.TryAdd(eventData)) continue;
                    
                    // Batch dolmuşsa gönder ve yeni batch oluştur
                    await producerClient.SendAsync(eventBatch);
                    Console.WriteLine("Batch gönderildi.");

                    eventBatch = await producerClient.CreateBatchAsync();
                    eventBatch.TryAdd(eventData);
                }

                // Son batch'i gönder
                if (eventBatch.Count > 0)
                {
                    await producerClient.SendAsync(eventBatch);
                    Console.WriteLine("Son batch gönderildi.");
                }
            }

            await Task.Delay(100); // Kuyruğu düzenli kontrol etmek için kısa bir bekleme
        }
        
        Console.WriteLine("ProcessQueueAsync durduruldu.");
    }
    
    
    public async Task SendEventWithRetryAsync(string message)
    {
        // Retry politikalarını tanımlıyoruz
        var producerOptions = new EventHubProducerClientOptions
        {
            RetryOptions = new EventHubsRetryOptions
            {
                Mode = EventHubsRetryMode.Exponential,       // Denemeler arasında üstel olarak bekleme süresi artar
                Delay = TimeSpan.FromSeconds(2),              // Başlangıçtaki bekleme süresi
                MaximumRetries = 5,                           // Maksimum tekrar sayısı
                MaximumDelay = TimeSpan.FromSeconds(30)       // Maksimum bekleme süresi
            }
        };
        
        await using var producerClient = new EventHubProducerClient(connectionString, eventHubName, producerOptions);
        
        try
        {
            using var eventBatch = await producerClient.CreateBatchAsync();

            // Mesajı batch'e ekleme
            if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(message))))
                throw new Exception("Event is too large for the batch.");

            // Event Hub'a gönderim
            await producerClient.SendAsync(eventBatch);
            Console.WriteLine("Event başarıyla gönderildi.");
        }
        catch (EventHubsException ex) when (ex.Reason == EventHubsException.FailureReason.ServiceBusy)
        {
            Console.WriteLine("Event Hubs şu anda meşgul. Lütfen bir süre sonra tekrar deneyin.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Gönderim sırasında bir hata oluştu: {ex.Message}");
            // Loglama veya başka bir hata işleme süreci ekleyebilirsiniz
        }
    }
}