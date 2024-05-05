using darq;
using FASTER.client;
using FASTER.darq;

namespace FASTER.server
{
    /// <summary>
    /// Options when creating DARQ server
    /// </summary>
    public class DarqServerOptions
    {
        /// <summary>
        /// Port to run server on.
        /// </summary>
        public int Port = 3278;

        /// <summary>
        /// IP address to bind server to.
        /// </summary>
        public string Address = "127.0.0.1";
        
        public IDarqClusterInfo ClusterInfo;
        
        public DarqSettings DarqSettings;
    }
}