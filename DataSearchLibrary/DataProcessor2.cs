using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FuzzySharp;
using Soundex;

namespace DataProcessorLibrary2
{
    public class DataProcessor
    {
        private string _filePath;
        private DateTime _lastModified;
        private List<List<(string ID, string Name,string SoundexCode)>> _dataChunks;
        private int _chunkSize = 100000; // Adjust chunk size as needed
        DateTime now = DateTime.Now; // Gets the current local date and time.

        public DataProcessor(string filePath, int chunkSize)
        {
            _filePath = filePath;
            _chunkSize = chunkSize;
            _dataChunks = new List<List<(string ID, string Name, string SoundexCode)>>();
        }



        public async Task LoadDataParallelAsync()
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            _dataChunks.Clear();
            Console.WriteLine("Parallel file upload started : " + _chunkSize);
            try
            {
                using (var fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true))
                {
                    long fileLength = fileStream.Length;
                    long chunkCount = (fileLength + _chunkSize - 1) / _chunkSize; // Round up

                    await Parallel.ForEachAsync(Enumerable.Range(0, (int)chunkCount), async (chunkIndex, token) =>
                    {
                        long startPosition = chunkIndex * _chunkSize;
                        long endPosition = Math.Min(startPosition + _chunkSize, fileLength);
                        long actualChunkSize = endPosition - startPosition;

                        byte[] buffer = new byte[actualChunkSize];
                        fileStream.Seek(startPosition, SeekOrigin.Begin);
                        await fileStream.ReadAsync(buffer, 0, (int)actualChunkSize);

                        string chunkData = System.Text.Encoding.UTF8.GetString(buffer); // Assuming UTF-8 encoding.
                        var processedChunk = ProcessChunk(chunkData);

                        lock (_dataChunks)
                        {
                            _dataChunks.Add(processedChunk);
                        }
                    });
                }

                stopwatch.Stop();
                Console.WriteLine($"Parallel file load took: {stopwatch.ElapsedMilliseconds} ms");
                Console.WriteLine("Paralle file upload ended");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading data: {ex.Message}");
            }
        }


        // Moved ProcessChunk to the class level:
        private List<(string ID, string Name, string SoundexCode)> ProcessChunk(string chunkData)
        {
            var results = new List<(string ID, string Name, string SoundexCode)>();
            using (var reader = new StringReader(chunkData))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(',');
                    if (parts.Length == 2)
                    {
                        string id = parts[0];
                        string name = parts[1];
                        string soundexCode = SoundexAlgorithm.GetSoundex(name);
                        results.Add((id, name, soundexCode));
                    }
                }
            }
            return results;
        }






        private async Task LoadDataAsync()
        {
            Stopwatch dataloadtimer = Stopwatch.StartNew();
            _dataChunks.Clear();

            try
            {
                using (var stream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, useAsync: true)) // Increase buffer size and enable async
                using (var reader = new StreamReader(stream))
                {
                    await reader.ReadLineAsync(); // Skip header

                    string line;
                    var currentChunk = new List<(string ID, string Name, string SoundexCode)>();
                    int count = 0;

                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        var lineSpan = line.AsSpan(); // Use ReadOnlySpan for efficient string processing.
                        int commaIndex = lineSpan.IndexOf(',');

                        if (commaIndex > 0 && commaIndex < lineSpan.Length - 1) // Basic validation
                        {
                            string id = lineSpan.Slice(0, commaIndex).ToString();
                            string name = lineSpan.Slice(commaIndex + 1).ToString();
                            string soundexCode = SoundexAlgorithm.GetSoundex(name);

                            currentChunk.Add((id, name, soundexCode));
                            count++;

                            if (count % _chunkSize == 0)
                            {
                                _dataChunks.Add(currentChunk);
                                currentChunk = new List<(string ID, string Name, string SoundexCode)>();
                            }
                        }
                    }

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

public async Task LoadDataIfChangedAsync()
        {
            var fileInfo = new FileInfo(_filePath);
            if (fileInfo.LastWriteTimeUtc > _lastModified)
            {
                _lastModified = fileInfo.LastWriteTimeUtc;
                Console.WriteLine("File Reading: " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                //await LoadDataAsync();
                await LoadDataParallelAsync();
                Console.WriteLine("File Loaded to chunks: " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
            }
        }

        public List<(string ID, string Name, int Score)> Search(string searchName)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            ConcurrentBag<(string ID, string Name, int Score)> results = new ConcurrentBag<(string ID, string Name, int Score)>();
            string searchSoundex = SoundexAlgorithm.GetSoundex(searchName);

            Parallel.ForEach(_dataChunks, chunk =>
            {
                foreach (var item in chunk)
                {
                    int fuzzyScore = Fuzz.PartialRatio(item.Name, searchName);
                    if (fuzzyScore > 80)
                    {
                        int soundexScore = item.SoundexCode == searchSoundex ? 100 : 0;
                        int compositeScore = (fuzzyScore + soundexScore) / 2;
                        if (compositeScore > 80)
                        {
                            results.Add((item.ID, item.Name, compositeScore));
                        }
                    }
                }
            });

            stopwatch.Stop();
            Console.WriteLine($"Search took: {stopwatch.ElapsedMilliseconds} ms");
            return results.ToList();
        }
    }
}