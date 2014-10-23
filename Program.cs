using System;
using System.IO;
using System.Linq;
using System.Threading;
using CsvHelper;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using StackExchange.Redis;

namespace RedisLatencyChecker
{
	internal class Program
	{
		static readonly ILog Log = LogManager.GetLogger(typeof (Program));

		static void Main(string[] args)
		{
			ConfigureLogging();

			Log.Info("Starting Up");

			// Create Servers
			var servers = args.Select(x => new Server(x)).ToArray();

			// Sanity Check
			if (servers.Length == 0) {
				Log.Error("Usage: RedisLatencyChecker <\"ConnectionString1\" [\"ConnectionString2\" ...]>");
				return;
			}

			// Print Diagnostics
			foreach(var server in servers)
				Log.Info(string.Format("Monitoring: {0}", server.ConnString));

			// Start Latency Checking
			const string SubDirectory = "Results";
			Directory.CreateDirectory(SubDirectory);
			var filename = string.Format("{0}/{1}.csv", SubDirectory, DateTime.Now.ToString("yyyy-MM-dd_HHmmss"));
			using (var streamWriter = new StreamWriter(filename)) {
				streamWriter.AutoFlush = true;

				using (var writer = new CsvWriter(streamWriter)) {
					// Header
					writer.WriteField("UtcTimestamp");
					for (var i = 0; i < servers.Length; i++) {
						writer.WriteField(string.Format("Server{0}Problem", i));
						writer.WriteField(string.Format("Server{0}Ping", i));
						writer.WriteField(string.Format("Server{0}Error", i));
					}
					writer.NextRecord();

					// Ping Servers
					while (true) {
						writer.WriteField(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss"));

						for (var i = 0; i < servers.Length; i++) {
							var server = servers[i];

							// Ping Server
							Exception exception = null;
							TimeSpan? elapsed = null;
							try {
								elapsed = server.Multiplexer.GetDatabase().Ping();
								Log.Info(string.Format("Ping() completed for Server {0} in {1}ms", server.ConnString,
									elapsed.Value.TotalMilliseconds));
							} catch (Exception ex) {
								exception = ex;
								Log.Error(string.Format("Ping() failed for Server {0} with {1}", server.ConnString, exception));
							}

							// Write
							writer.WriteField(exception != null ? 1 : 0);
							writer.WriteField(elapsed != null ? (double?) elapsed.Value.TotalMilliseconds : null);
							writer.WriteField(exception != null ? exception.ToString() : null);
						}

						// Flush
						writer.NextRecord();

						// Wait
						Thread.Sleep(1000);
					}
				}
			}
		}

		static void ConfigureLogging()
		{
			var consoleAppender = new ConsoleAppender();
			consoleAppender.Layout = new PatternLayout("[%date{yyyy-MM-dd HH:mm:ss}] %-5p %c{1} - %m%n");

			BasicConfigurator.Configure(consoleAppender);
		}

		class Server
		{
			public Server(string connString)
			{
				ConnString = connString;
				Logger = new StringWriter();

				var options = ConfigurationOptions.Parse(connString);
				Multiplexer = ConnectionMultiplexer.Connect(options, Logger);
			}

			public StringWriter Logger { get; set; }

			public string ConnString { get; set; }
			public ConnectionMultiplexer Multiplexer { get; set; }
		}
	}
}