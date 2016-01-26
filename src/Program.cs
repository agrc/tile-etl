using System;
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Storage.v1;
using Google.Apis.Storage.v1.Data;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace tile_etl
{
    internal class Program
    {
        private const string CertificateFile = @"C:\certificates\Utah Imagery-ab8dd8c09894.p12";
        private const string ServiceAccountEmail =
            "344455572219-vhfqhg6iljbulqqnc5prh2ltmifrume4@developer.gserviceaccount.com";

        private static string _tileDirectory;
        private static string _mapName;

        [STAThread]
        private static void Main(string[] args)
        {
            _mapName = args.Length < 1 ? null : args[0];
            
            if (_mapName == null)
            {
                Console.Write("Map Name (e.g. 'BaseMaps_WGS_Topo'): ");
                _mapName = Console.ReadLine();
            }

            _tileDirectory = string.Format(@"C:\arcgisserver\directories\arcgiscache\{0}\Layers\_alllayers", _mapName);

            var certificate = new X509Certificate2(CertificateFile, "notasecret", X509KeyStorageFlags.Exportable);

            var credential = new ServiceAccountCredential(new ServiceAccountCredential.Initializer(ServiceAccountEmail)
            {
                Scopes = new[]
                {
                    StorageService.Scope.DevstorageReadWrite
                }
            }.FromCertificate(certificate));

            var aclList = new[]
            {
                new ObjectAccessControl
                {
                    Entity = "allUsers",
                    Role = "OWNER"
                }
            };

            var acl = new ArraySegment<ObjectAccessControl>(aclList);

            foreach (var folder in Directory.GetDirectories(_tileDirectory))
            {
                try
                {
                    Run(new DirectoryInfo(folder), credential, acl);
                }
                catch (AggregateException ex)
                {
                    foreach (var err in ex.InnerExceptions)
                    {
                        Console.WriteLine("ERROR: " + err.Message);
                    }
                }
            }
        }

        public static void Run(DirectoryInfo folder, ServiceAccountCredential credential, ArraySegment<ObjectAccessControl> acl )
        {
            Console.WriteLine("processing level {0}", folder.Name);
            Debug.WriteLine("processing level {0}", folder.Name);

            var level = int.Parse(folder.Name.Remove(0, 1));
            Parallel.ForEach(folder.GetDirectories(), new ParallelOptions
            {
                MaxDegreeOfParallelism = 75
            }, rowDirectory =>
            {
                var row = int.Parse(rowDirectory.Name.Remove(0, 1), NumberStyles.HexNumber);
                Console.WriteLine("processing row {0}", rowDirectory.Name);
                Debug.WriteLine("Processing row {0}", rowDirectory.Name);
                using (var service = new StorageService(new BaseClientService.Initializer
                {
                    HttpClientInitializer = credential,
                    ApplicationName = "Tile-ETL"
                }))
                {
                    foreach(var column in rowDirectory.GetFiles())
                    {
                        try
                        {
                            UploadFile(column.FullName,
                                       level,
                                       int.Parse(column.Name.Substring(1, column.Name.Length - 5), NumberStyles.HexNumber),
                                       row,
                                       acl,
                                       service,
                                       column.Name.Substring(column.Name.IndexOf(".") + 1, 3));
                        }
                        catch (AggregateException ex)
                        {
                            foreach (var err in ex.InnerExceptions)
                            {
                                Console.WriteLine("ERROR: " + err.Message);
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
            });

            Console.WriteLine("finished processing {0}", folder.Name);
        }

        private static void UploadFile(string imagePath, int level, int column, int row, ArraySegment<ObjectAccessControl> acl, StorageService service, string imageType)
        {
            Debug.WriteLine("uploading " + imagePath);

            var file = File.ReadAllBytes(imagePath);
            var configs = ConfigurationManager.AppSettings[_mapName].Split(';');
            var bucket = configs[0];
            var folder = configs[1];

            using (var streamOut = new MemoryStream(file))
            {
                var fileobj = new Object
                {
                    Name = string.Format("{0}/{1}/{2}/{3}", folder, level, column, row),
                    Acl = acl
                };

                service.Objects.Insert(fileobj, bucket, streamOut, "image/" + imageType).Upload();
            }
        }
    }
}