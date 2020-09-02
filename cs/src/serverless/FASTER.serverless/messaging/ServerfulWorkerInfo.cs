using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace FASTER.serverless
{
    /// <inheritdoc />
    [Serializable]
    public class ServerfulWorkerInfo : IWorkerInfo
    {
        private long serverId;
        private string address;
        private int port;

        /// <summary>
        /// 
        /// </summary>
        public ServerfulWorkerInfo() { }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="serverId"></param>
        /// <param name="address"></param>
        /// <param name="port"></param>
        public ServerfulWorkerInfo(long serverId, string address, int port)
        {
            this.serverId = serverId;
            this.address = address;
            this.port = port;
        }

        /// <inheritdoc />
        public Worker GetWorker()
        {
            return new Worker(serverId);
        }

        /// <inheritdoc />
        public void InitializeFromByteArray(byte[] representation)
        {
            var deserializer = new BinaryFormatter();
            var info = (ServerfulWorkerInfo) deserializer.Deserialize(new MemoryStream(representation));
            serverId = info.serverId;
            address = info.address;
            port = info.port;
        }

        /// <inheritdoc />
        public byte[] AsByteArray()
        {
            using (var result = new MemoryStream())
            {
                var serializer = new BinaryFormatter();
                serializer.Serialize(result, this);
                return result.ToArray();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetAddress()
        {
            return address;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int GetPort()
        {
            return port;
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == this.GetType() && serverId.Equals(((ServerfulWorkerInfo) obj).serverId);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return serverId.GetHashCode();
        }
    }
}