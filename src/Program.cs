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

namespace tile_etl
{
    internal class Program
    {
        private const string BucketName = "state-of-utah-lite-tiles";
        private const string MapName = "Lite";

        private const string CertificateFile = @"C:\certificates\Utah Imagery-ab8dd8c09894.p12";

        private const string ServiceAccountEmail =
            "344455572219-vhfqhg6iljbulqqnc5prh2ltmifrume4@developer.gserviceaccount.com";

        private const int StartLevel = 13;
        private const int EndLevel = 19;
        private const double ExtentMinX = -14078565;
        private const double ExtentMinY = 3604577;
        private const double ExtentMaxX = -11137983;
        private const double ExtentMaxY = 6384021;
        private const double WebMercatorDelta = 20037508.34278;
        private const string TileDirectory = @"C:\arcgisserver\directories\arcgiscache\Lite_WGS\Layers\_alllayers";

        [STAThread]
        private static void Main()
        {
            try
            {
                new Program().Run();
            }
            catch (AggregateException ex)
            {
                foreach (var err in ex.InnerExceptions)
                {
                    Console.WriteLine("ERROR: " + err.Message);
                }
            }
        }

        public void Run()
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

            Parallel.For(StartLevel, EndLevel+1, level =>
            {
                Console.WriteLine("processing level {0}", level);
                Debug.WriteLine("processing level {0}", level);
                var tileSize = WebMercatorDelta*Math.Pow(2, 1 - level);

                var startRow = Convert.ToInt32(Math.Truncate((WebMercatorDelta - ExtentMaxY)/tileSize));
                var endRow = Convert.ToInt32(Math.Truncate((WebMercatorDelta - ExtentMinY)/tileSize)) + 1;
                var localLevel = level;

                Parallel.For(startRow, endRow, r =>
                {
                    Debug.WriteLine("Processing row {0:x8}", r);
                    var startColumn = Convert.ToInt32(Math.Truncate((ExtentMinX + WebMercatorDelta)/tileSize));
                    var endColumn = Convert.ToInt32(Math.Truncate((ExtentMaxX + WebMercatorDelta)/tileSize)) + 1;

                    var stopWatch = Stopwatch.StartNew();
                    var numberOfFiles = 0;
                    
                    var service = new StorageService(new BaseClientService.Initializer
                    {
                         HttpClientInitializer = credential,
                         ApplicationName = "Tile-ETL"
                    });

                    for (var c = startColumn; c <= endColumn; ++c)
                    {
                        Debug.WriteLine("Processing column {0:x8}", c);

                        try
                        {
                            var imagePath = string.Format("{0}\\L{1:00}\\R{2:x8}\\C{3:x8}.{4}", TileDirectory,
                                localLevel,
                                r, c, "jpg");

                            if (File.Exists(imagePath))
                            {
                                Debug.WriteLine("{0}/{1}/{2}/{3}", MapName, localLevel, c, r);
                                Debug.WriteLine("{0}/{1}/{2:x8}/{3:x8}", MapName, localLevel, c, r);

                                var file = File.ReadAllBytes(imagePath);

                                using (var streamOut = new MemoryStream(file))
                                {
                                    var fileobj = new Object
                                    {
                                        Name = string.Format("{0}/{1}/{2}/{3}", MapName, localLevel, c, r),
                                        Acl = acl
                                    };

                                    service.Objects.Insert(fileobj, BucketName, streamOut, "image/jpg").Upload();
                                    numberOfFiles += 1;
                                }
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

                    stopWatch.Stop();
                    if (numberOfFiles > 0)
                    {
                        Console.WriteLine("Finished row {0} for level {1} in {2}ms number of files {3}", r, level,
                            stopWatch.ElapsedMilliseconds, numberOfFiles);
                        Debug.WriteLine("Finished row {0} for level {1} in {2}ms number of files {3}", r, level,
                            stopWatch.ElapsedMilliseconds, numberOfFiles);
                    }
                });

                Console.WriteLine("finished processing {0}", level);
                Debug.WriteLine("finished processing {0}", level);
            });
        }
    }
}