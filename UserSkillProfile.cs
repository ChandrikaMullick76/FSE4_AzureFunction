using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace UserSkillProfiles.Models
{
    public class UserSkillProfile
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string UserID { get; set; }

        [BsonElement("Name")]
        public string Name { get; set; }

        [BsonElement("AssociateId")]
        public string AssociateId { get; set; }

        [BsonElement("Mobile")]
        public string Mobile { get; set; }

        [BsonElement("Email")]
        public string Email { get; set; }

        [BsonElement("HTMLCSSJAVASCRIPT")]
        public int HTMLCSSJAVASCRIPT { get; set; }

        [BsonElement("ANGULAR")]
        public int ANGULAR { get; set; }

        [BsonElement("REACT")]
        public int REACT { get; set; }

        [BsonElement("AspNetCore")]
        public int AspNetCore { get; set; }

        [BsonElement("RESTFUL")]
        public int RESTFUL { get; set; }

        [BsonElement("EntityFramework")]
        public int EntityFramework { get; set; }

        [BsonElement("GIT")]
        public int GIT { get; set; }

        [BsonElement("DOCKER")]
        public int DOCKER { get; set; }

        [BsonElement("JENKINS")]
        public int JENKINS { get; set; }

        [BsonElement("Azure")]
        public int Azure { get; set; }

        [BsonElement("SPOKEN")]
        public int SPOKEN { get; set; }

        [BsonElement("COMMUNICATION")]
        public int COMMUNICATION { get; set; }

        [BsonElement("APTITUDE")]
        public int APTITUDE { get; set; }

        [BsonElement("CreationDate")]
        public string CreationDate { get; set; }

        internal UserSkillProfile FirstOrDefault()
        {
            throw new NotImplementedException();
        }
    }
}
