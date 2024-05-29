using MongoDB.Bson;
using MongoDB.Driver;
using Polly;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDbFailoverSimulation;

class Employee
{
    public int id { get; set; }
    public string name { get; set; }
}

class Program
{
    private static IMongoClient _mongoClient;
    private static IMongoDatabase _mongoDatabase;
    private static IMongoCollection<Employee>? _collection;

    static void Main(string[] args)
    {
        string connectionString = "";
        MongoClientSettings clientSettings = MongoClientSettings.FromConnectionString(connectionString);

        _mongoClient = new MongoClient(clientSettings);
        _mongoDatabase = _mongoClient.GetDatabase("SPPIT");
        _collection = _mongoDatabase.GetCollection<Employee>("Employees");
    }

    static async Task InsertRecords()
    {
        var retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(5, retryAttempt => TimeSpan.FromSeconds(2),
                (exception, timeSpan, retryCount, context) => Console.WriteLine(
                    $"Insert attempt {retryCount} failed with exception: {exception.Message}. Retrying in {timeSpan}..."));

        for (int i = 0; i < 300; i++)
        {
            var document = new Employee()
            {
                id = i,
                name = "Sid" + i
            };

            await retryPolicy.ExecuteAsync(async () =>
            {
                await _collection.InsertOneAsync(document);
                Console.WriteLine($"Inserted: {document}");
            });
            await Task.Delay(1000);
        }
    }

    static async Task ReadRecords()
    {
        while (true)
        {
            try
            {
                var documents = await _collection.Find(new BsonDocument()).ToListAsync();
                Console.WriteLine($"Read {documents.Count} records");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Read exception: {ex.Message}");
            }

            await Task.Delay(500); // 1/2 second delay
        }
    }

    static void SimulateFailover()
    {
        // Allow some time for initial inserts and reads
        Thread.Sleep(20000);

        // Kill primary node
        KillProcessOnPort(27017);
        Console.WriteLine("Primary node killed.");

        // Wait for failover
        Thread.Sleep(30000); // 30 seconds to allow failover

        // Restart primary node (assuming the process is mongod.exe and it is in the PATH)
        StartProcess("mongod", "--port 27017 --replSet rs0 --dbpath /data/db1");
        Console.WriteLine("Primary node restarted.");

        // Allow some time for re-election
        Thread.Sleep(20000);

        // Kill secondary node
        KillProcessOnPort(27018);
        Console.WriteLine("Secondary node killed.");

        // Wait for failover
        Thread.Sleep(30000); // 30 seconds to allow failover

        // Restart secondary node
        StartProcess("mongod", "--port 27018 --replSet rs0 --dbpath /data/db2");
        Console.WriteLine("Secondary node restarted.");

        // Allow some time for re-election
        Thread.Sleep(20000);

        // End the program after simulation
        Environment.Exit(0);
    }

    static void StartProcess(string fileName, string arguments)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = arguments,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using (var process = Process.Start(startInfo))
        {
            process.OutputDataReceived += (sender, e) => Console.WriteLine($"Output: {e.Data}");
            process.ErrorDataReceived += (sender, e) => Console.WriteLine($"Error: {e.Data}");

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            process.WaitForExit();
        }
    }

    static void KillProcessOnPort(int port)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = "cmd.exe",
            Arguments = $"/C netstat -ano | findstr :{port}",
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using (var process = Process.Start(startInfo))
        {
            var output = process.StandardOutput.ReadToEnd();
            process.WaitForExit();

            var lines = output.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines)
            {
                var parts = line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                var pid = parts[parts.Length - 1];
                var killStartInfo = new ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = $"/C taskkill /PID {pid} /F",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using (var killProcess = Process.Start(killStartInfo))
                {
                    killProcess.WaitForExit();
                }
            }
        }
    }
}