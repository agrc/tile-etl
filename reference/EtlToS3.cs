using System;
using System.Configuration;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System.Threading;

namespace AgsTilesToS3
{
  class Program
  {
    private const string TileDirectory = @"\\procyon\arcgiscache\MTC_basemap_Becket_WM\Layers\_alllayers";
    private const string BucketName = "AppGeo_MA";
    private const string MapName = "MBIAddressing/Basemap";
    private const string ContentType = "image/png";

    private const int StartLevel = 17;
    private const int EndLevel = 18;

    private const double ExtentMinX = -8143974;
    private const double ExtentMinY = 5195979;
    private const double ExtentMaxX = -8125992;
    private const double ExtentMaxY = 5212260;

    private const int TilePaddingX = 6;
    private const int TilePaddingY = 6;

    private const double WebMercatorDelta = 20037508.342787;

    private const int MaxThreads = 40;

    private static Semaphore _threadPool;
    private static AmazonS3 _s3Client;

    public static void Main(string[] args)
    {
      _threadPool = new Semaphore(MaxThreads, MaxThreads);

      using (_s3Client = AWSClientFactory.CreateAmazonS3Client(ConfigurationManager.AppSettings["AWSAccessKey"], ConfigurationManager.AppSettings["AWSSecretKey"]))
      {
        for (int level = StartLevel; level <= EndLevel; ++level)
        {
          double tileSize = WebMercatorDelta * Math.Pow(2, 1 - level);

          int startRow = Convert.ToInt32(Math.Truncate((WebMercatorDelta - ExtentMaxY) / tileSize)) - TilePaddingY;
          int endRow = Convert.ToInt32(Math.Truncate((WebMercatorDelta - ExtentMinY) / tileSize)) + 1 + TilePaddingY;
          int startColumn = Convert.ToInt32(Math.Truncate((ExtentMinX + WebMercatorDelta) / tileSize)) - TilePaddingX;
          int endColumn = Convert.ToInt32(Math.Truncate((ExtentMaxX + WebMercatorDelta) / tileSize)) + 1 + TilePaddingX;

          for (int r = startRow; r <= endRow; ++r)
          {
            for (int c = startColumn; c <= endColumn; ++c)
            {
              _threadPool.WaitOne();

              Thread t = new Thread(new ParameterizedThreadStart(CopyImage));
              t.Start(new UserData(level, r, c));

              Console.Write(String.Format("{0}Level {1} Row {2} Column {3}", new String('\b', 40), level, r, c).PadRight(80));
            }
          }
        }
      }

      Console.WriteLine((new String('\b', 40) + "Done").PadRight(80));
      Console.Read();
    }

    private static void CopyImage(object o)
    {
      UserData u = (UserData)o;

      try
      {
        string imagePath = String.Format("{0}\\L{1:00}\\R{2:x8}\\C{3:x8}.{4}", TileDirectory, u.Level, u.Row, u.Column, ContentType == "image/png" ? "png" : "jpg");

        if (File.Exists(imagePath))
        {
          byte[] file = File.ReadAllBytes(imagePath);

          PutObjectRequest putObjectRequest = new PutObjectRequest();
          putObjectRequest.WithBucketName(BucketName);
          putObjectRequest.WithKey(String.Format("{0}/{1}/{2}/{3}", MapName, u.Level, u.Row, u.Column));
          putObjectRequest.WithInputStream(new MemoryStream(file));
          putObjectRequest.WithContentType(ContentType);
          putObjectRequest.WithCannedACL(S3CannedACL.PublicRead);

          _s3Client.PutObject(putObjectRequest);
        }
      }
      catch (Exception ex)
      {
      }
      finally
      {
        _threadPool.Release();
      }
    }
  }

  public class UserData
  {
    public int Level;
    public int Row;
    public int Column;

    public UserData(int level, int row, int column)
    {
      Level = level;
      Row = row;
      Column = column;
    }
  }
}
