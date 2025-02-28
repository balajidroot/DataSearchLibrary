using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FuzzySharp;
using System.Text;

namespace DataSearchLibrary
{
    
    public class DataProcessor
    {
        private string _filePath;
        private DateTime _lastModified;
        private List<List<string[]>> _dataChunks;
        public int _chunkSize = 10000;
        public bool _initialized;


        public DataProcessor(string filePath)
        {
            _filePath = filePath;
            _dataChunks = new List<List<string[]>>();
            if (!_initialized)
            {
                _lastModified = DateTime.MinValue;
                _initialized = true;
            }
        }

        public async Task LoadDataIfChanged()
        {
            var fileInfo = new FileInfo(_filePath);

            Console.WriteLine(fileInfo);
            Console.WriteLine(_lastModified);
            if (fileInfo.LastWriteTimeUtc > _lastModified)
            {
                _lastModified = fileInfo.LastWriteTimeUtc;
                Console.WriteLine(fileInfo.LastWriteTimeUtc);
                await LoadAndChunkData();
            }
        }

        private async Task LoadAndChunkData()
        {
            _dataChunks.Clear();
            List<string[]> currentChunk = new List<string[]>();

            using (var reader = new StreamReader(_filePath))
            {
                reader.ReadLine(); // Skip header
                while (!reader.EndOfStream)
                {
                    string line = await reader.ReadLineAsync();
                    string[] values = line.Split(',');
                    currentChunk.Add(values);

                    if (currentChunk.Count >= _chunkSize)
                    {
                        _dataChunks.Add(currentChunk);
                        currentChunk = new List<string[]>();
                    }
                }

                if (currentChunk.Count > 0)
                {
                    _dataChunks.Add(currentChunk);
                }
            }
        }

        public async Task<List<string[]>> SearchData(string searchName, int chunkSize)
        {
            DateTime now = DateTime.Now; // Gets the current local date and time.
            Console.WriteLine("File Reading: " + now.ToString("yyyy-MM-dd HH:mm:ss"));

            _chunkSize = chunkSize;
            Console.WriteLine("chunk size set: " + _chunkSize);

            Stopwatch FileReadProcessingTimer = Stopwatch.StartNew();

            await LoadDataIfChanged();
            FileReadProcessingTimer.Stop();
            Console.WriteLine($"Filtering time: {FileReadProcessingTimer.ElapsedMilliseconds} ms");

            Console.WriteLine("File Reading Done: " + now.ToString("yyyy-MM-dd HH:mm:ss"));

            if (_dataChunks.Count == 0)
            {
                return new List<string[]>();
            }

            var results = new List<string[]>();
            var tasks = new List<Task>();
            object resultsLock = new object();

            Console.WriteLine("Processing chunk startg: " + now.ToString("yyyy-MM-dd HH:mm:ss"));
            Stopwatch chunkProcessingTimer = Stopwatch.StartNew();

            foreach (var chunk in _dataChunks)
            {
                tasks.Add(Task.Run(() =>
                {
                    var chunkResults = ProcessChunk(chunk, searchName);
                    lock (resultsLock)
                    {
                        results.AddRange(chunkResults);
                    }
                }));
            }

            await Task.WhenAll(tasks);
            chunkProcessingTimer.Stop();
            Console.WriteLine($"Chunk processing time: {chunkProcessingTimer.ElapsedMilliseconds} ms");
            Console.WriteLine("Processing chunk end: " + now);

            Stopwatch filterTimer = Stopwatch.StartNew();
            var filteredResults = results.Where(r => CalculateCompositeScore(r[1], searchName) > 80).ToList(); // Name is column 1
            filterTimer.Stop();
            Console.WriteLine($"Filtering time: {filterTimer.ElapsedMilliseconds} ms");
            Console.WriteLine("filtering chunk end: " + now.ToString("yyyy-MM-dd HH:mm:ss"));

            return filteredResults;
        }

        private List<string[]> ProcessChunk(List<string[]> chunk, string searchName)
        {
            var chunkResults = new List<string[]>();
            foreach (var row in chunk)
            {
                int score = CalculateCompositeScore(row[1], searchName); // Name is column 1
                if (score > 80)
                {
                    chunkResults.Add(row);
                }
            }
            return chunkResults;
        }

        private int CalculateCompositeScore(string dataName, string searchName)
        {
            Stopwatch fuzzyTimer = Stopwatch.StartNew();
            int fuzzyScore = Fuzz.PartialRatio(dataName, searchName);
            fuzzyTimer.Stop();
            //Console.WriteLine($"Fuzzy time: {fuzzyTimer.ElapsedMilliseconds} ms");

            Stopwatch soundexTimer = Stopwatch.StartNew();
            int soundexScore = CalculateSoundexScore(dataName, searchName);
            soundexTimer.Stop();
            //Console.WriteLine($"Soundex time: {soundexTimer.ElapsedMilliseconds} ms");

            return (int)((fuzzyScore * 0.7) + (soundexScore * 0.3));
        }

        private int CalculateSoundexScore(string dataName, string searchName)
        {
            string dataSoundex = Soundex(dataName);
            string searchSoundex = Soundex(searchName);

            return dataSoundex == searchSoundex ? 100 : 0;
        }

        private string Soundex(string s)
        {
            if (string.IsNullOrEmpty(s)) return "";

            s = s.ToUpperInvariant();
            char firstChar = s[0];
            string soundex = firstChar.ToString();

            string encoded = s.Substring(1).Replace('H', '\0').Replace('W', '\0');
            encoded = encoded.Replace('B', '1').Replace('F', '1').Replace('P', '1').Replace('V', '1');
            encoded = encoded.Replace('C', '2').Replace('G', '2').Replace('J', '2').Replace('K', '2').Replace('Q', '2').Replace('S', '2').Replace('X', '2').Replace('Z', '2');
            encoded = encoded.Replace('D', '3').Replace('T', '3');
            encoded = encoded.Replace('L', '4');
            encoded = encoded.Replace('M', '5').Replace('N', '5');
            encoded = encoded.Replace('R', '6');

            StringBuilder sb = new StringBuilder();
            sb.Append(firstChar);

            char previousCode = '\0';
            foreach (char c in encoded)
            {
                if (c != '\0' && c != previousCode)
                {
                    sb.Append(c);
                    previousCode = c;
                }
            }

            return sb.ToString().PadRight(4, '0').Substring(0, 4);
        }
    }
}