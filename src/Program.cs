using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Storage.v1;
using Google.Apis.Storage.v1.Data;
using Object = Google.Apis.Storage.v1.Data.Object;
using System.Configuration;

namespace tile_etl
{
    internal class Program
    {
        private const string CertificateFile = @"C:\certificates\Utah Imagery-ab8dd8c09894.p12";
        private const string ServiceAccountEmail =
            "344455572219-vhfqhg6iljbulqqnc5prh2ltmifrume4@developer.gserviceaccount.com";

        private const double ExtentMinX = -14078565;
        private const double ExtentMinY = 3604577;
        private const double ExtentMaxX = -11137983;
        private const double ExtentMaxY = 6384021;
        private const double WebMercatorDelta = 20037508.34278;
        private static string tileDirectory;
        private static string mapName;

        [STAThread]
        private static void Main(string[] args)
        {
            mapName = args.Length < 1 ? null : args[0];
            
            if (mapName == null)
            {
                Console.Write("Map Name (e.g. 'BaseMaps_WGS_Topo'): ");
                mapName = Console.ReadLine();
            }

            tileDirectory = string.Format(@"C:\arcgisserver\directories\arcgiscache\{0}\Layers\_alllayers", mapName);

            foreach (string folder in Directory.GetDirectories(tileDirectory))
            {
                int level = int.Parse(folder.Substring(folder.Length - 2, 2));
                try
                {
                    new Program().Run(level);
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

        public void Run(int level)
        {
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

            Console.WriteLine("processing level {0}", level);
            Debug.WriteLine("processing level {0}", level);
            var tileSize = WebMercatorDelta*Math.Pow(2, 1 - level);

            var startRow = Convert.ToInt32(Math.Truncate((WebMercatorDelta - ExtentMaxY)/tileSize));
            var endRow = Convert.ToInt32(Math.Truncate((WebMercatorDelta - ExtentMinY)/tileSize)) + 1;

            Parallel.For(startRow, endRow, new ParallelOptions()
            {
                MaxDegreeOfParallelism = 75
            }, r =>
            {
                Debug.WriteLine("Processing row {0:x8}", r);
                var startColumn = Convert.ToInt32(Math.Truncate((ExtentMinX + WebMercatorDelta)/tileSize));
                var endColumn = Convert.ToInt32(Math.Truncate((ExtentMaxX + WebMercatorDelta)/tileSize)) + 1;

                var numberOfFiles = 0;

                using (var service = new StorageService(new BaseClientService.Initializer
                {
                    HttpClientInitializer = credential,
                    ApplicationName = "Tile-ETL"
                }))
                {
                    for (var c = startColumn; c <= endColumn; ++c)
                    {
                        try
                        {
                            var imagePath = string.Format("{0}\\L{1:00}\\R{2:x8}\\C{3:x8}.{4}", tileDirectory,
                                level,
                                r, c, "jpg");

                            if (File.Exists(imagePath))
                            {
                                uploadFile(imagePath, level, c, r, acl, service, "jpg");
                                numberOfFiles += 1;
                            }
                            else if (File.Exists(imagePath.Replace("jpg", "png")))
                            {
                                uploadFile(imagePath.Replace("jpg", "png"), level, c, r, acl, service, "png");
                                numberOfFiles += 1;
                            }
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

                    if (numberOfFiles > 0)
                    {
                        Console.WriteLine("{3}: Finished row {0} for level {1} number of files {2}", r, level,
                             numberOfFiles, mapName);
                    }
                }
            });

            Console.WriteLine("finished processing {0}", level);
        }

        private void uploadFile(string imagePath, int level, int column, int row, ArraySegment<ObjectAccessControl> acl, StorageService service, string imageType)
        {
            Debug.WriteLine("uploading " + imagePath);

            var file = File.ReadAllBytes(imagePath);
            var configs = ConfigurationManager.AppSettings[mapName].Split(';');
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