// DataProcessor.cs (DLL)

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FuzzySharp;
using Soundex;
using System.Threading.Tasks;

namespace DataProcessorLibrary1
{
    public class DataProcessor
    {
        private string _filePath;
        private DateTime _lastModified;
        private List<List<(string ID, string Name, string soundexcode)>> _dataChunks;
        private int _chunkSize = 100000; // Adjust chunk size as needed
        DateTime now = DateTime.Now; // Gets the current local date and time.

        public DataProcessor(string filePath,int chunkSize)
        {
            _filePath = filePath;
            _chunkSize = chunkSize;
            _dataChunks = new List<List<(string ID, string Name, string soundexcode)>>();
        }

        public void LoadDataIfChanged()
        {
            var fileInfo = new FileInfo(_filePath);
            Console.WriteLine(fileInfo.LastWriteTimeUtc + "  vs " +_lastModified);
            if (fileInfo.LastWriteTimeUtc > _lastModified)
            {
                _lastModified = fileInfo.LastWriteTimeUtc;
                
                Console.WriteLine("File Reading: " + now.ToString("yyyy-MM-dd HH:mm:ss"));
                LoadData();
                Console.WriteLine("File Loaded to chunks: " + now.ToString("yyyy-MM-dd HH:mm:ss"));

            }
        }

        private void LoadData()
        {
            Stopwatch dataloadtimer = Stopwatch.StartNew();
            Console.WriteLine("File Loaded to chunks: " + _dataChunks.Count);
            _dataChunks.Clear();
            try
            {
                using (var reader = new StreamReader(_filePath))
                {
                    reader.ReadLine(); // Skip header
                    string line;
                    var currentChunk = new List<(string ID, string Name, string soundexcode)>();
                    int count = 0;
                    while ((line = reader.ReadLine()) != null)
                    {
                        var parts = line.Split(',');
                        if (parts.Length == 2)//&& int.TryParse(parts[0], out string ID)
                        {
                            currentChunk.Add((parts[0], parts[1], SoundexAlgorithm.GetSoundex(parts[1])));
                            count++;
                            if (count % _chunkSize == 0)
                            {
                                _dataChunks.Add(currentChunk);
                                currentChunk = new List<(string ID, string Name, string soundexcode)>();
                            }
                        }
                    }
                    Console.WriteLine("Total Line Count " + count);
                    if (currentChunk.Count > 0)
                    {
                        _dataChunks.Add(currentChunk);
                    }

                }
                dataloadtimer.Stop();
                Console.WriteLine($"Process File took: {dataloadtimer.ElapsedMilliseconds} ms");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading data: {ex.Message}");
            }
        }

        public List<(string ID, string Name, int Score)> Search(string searchName)
        {

            ParallelOptions parallelOptions = new ParallelOptions();
            // Set the maximum degree of parallelism. Use -1 for unlimited.
            // You can set it to a specific number, e.g., Environment.ProcessorCount, or a custom value.
            parallelOptions.MaxDegreeOfParallelism = 100;
            Stopwatch stopwatch = Stopwatch.StartNew();
            var soundexscore = SoundexAlgorithm.GetSoundex(searchName);
            List<(string ID, string Name, int Score)> results = new List<(string ID, string Name, int Score)>();
            Parallel.ForEach(_dataChunks, parallelOptions, chunk =>
            {
                foreach (var item in chunk)
                {
                    int fuzzyScore = Fuzz.PartialRatio(item.Name, searchName);
                    //int soundexScore = SoundexAlgorithm.GetSoundex(item.Name) == soundexscore ? 100 : 0;
                    int soundexScore = item.soundexcode == soundexscore ? 100 : 0;
                    int compositeScore = (fuzzyScore + soundexScore) / 2;
                    if (compositeScore > 80)
                    {
                        lock (results)
                        {
                            results.Add((item.ID, item.Name, compositeScore));
                        }
                    }
                }
            });
            stopwatch.Stop();
            Console.WriteLine($"Search took: {stopwatch.ElapsedMilliseconds} ms");
            return results;
        }
    }
}