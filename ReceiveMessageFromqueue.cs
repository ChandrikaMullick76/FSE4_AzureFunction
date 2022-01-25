using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace ReceiveMessageFromQueue
{
    public static class ReceiveMessageFromqueue
    {
        [FunctionName("ReceiveMessageFromqueue")]
        public static void Run([ServiceBusTrigger("userskillprofilequeue", Connection = "AzureBusConnection")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");
        }
    }
}
