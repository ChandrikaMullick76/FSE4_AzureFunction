using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Newtonsoft.Json;
using UserSkillProfiles.Models;

namespace ReceiveMessageFromQueue
{
    public static class ReceiveMessageFromqueue
    {
       
        public static object IMongodatabase { get; private set; }

        [FunctionName("ReceiveMessageFromqueue")]
        public static void Run([ServiceBusTrigger("userskillprofilequeue", Connection = "AzureBusConnection")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");
            String[] messageQueue = myQueueItem.Split('|');
             UserSkillProfile userData = JsonConvert.DeserializeObject<UserSkillProfile>(messageQueue[0]);


            //CreateUserSkilProfile(userData);
            if(messageQueue[1]!="ADD")
            {
                UpdateUserSkilProfile(userData.UserID, userData);
            }
            else
            {
                CreateUserSkilProfile(userData);
            }


        }

        static void CreateUserSkilProfile(UserSkillProfile userData)
        {
            //bool isSuccess = false;
            var client = new MongoClient("mongodb://adminsearchprofile:vRb1YY32GLlaqbm67W2pCRPJYJZHSSNtF9FbY9lwz9AUzbY4PTTWTXVkRdAaXHxiFHPVKy3tVlTYrcIefsH7mw==@adminsearchprofile.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@adminsearchprofile@");
            var database = client.GetDatabase("AdminSearchProfileDB");
            IMongoCollection<UserSkillProfile> _userSkillProfileData = database.GetCollection<UserSkillProfile>("UserSkillProfile");

            try
            {
                var obj = userData;
                _userSkillProfileData.InsertOne(userData);
               
            }
            catch (Exception ex)
            {

            }

            
        }

        static void UpdateUserSkilProfile(string userID, UserSkillProfile userData)
        {
            bool isSuccess = false;
            var client = new MongoClient("mongodb://adminsearchprofile:vRb1YY32GLlaqbm67W2pCRPJYJZHSSNtF9FbY9lwz9AUzbY4PTTWTXVkRdAaXHxiFHPVKy3tVlTYrcIefsH7mw==@adminsearchprofile.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@adminsearchprofile@");
            var database = client.GetDatabase("AdminSearchProfileDB");
            IMongoCollection<UserSkillProfile> _userSkillProfileData = database.GetCollection<UserSkillProfile>("UserSkillProfile");


            try
            {
                // var filter = Builders<UserSkillProfile>.Filter.Eq("UserID", userID);
                // & Builders<BuyerBidDetails>.Filter.Eq("ProductID", productID);
                //var update = Builders<UserSkillProfile>.Update.Set("HTMLCSSJAVASCRIPT", userData.HTMLCSSJAVASCRIPT);
                // _userSkillProfileData.UpdateOne(filter, update);
                _userSkillProfileData.ReplaceOne(x => x.UserID == userID, userData);

                isSuccess = true;
            }
            catch (Exception ex)
            {

            }

           // return isSuccess;
        }

    }
}
