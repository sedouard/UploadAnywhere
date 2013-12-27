using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Uploader.SeviceBusMessaging
{
    public class FileUploadedMessage
    {
        public string FileName { get; set; }
        public DateTime UploadTime { get; set; }
        //Each team has a cloud container. The container name is the name of the team
        public string ContainerName { get; set; }
    }
}
