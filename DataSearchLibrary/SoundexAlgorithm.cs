//SoundexAlgorithm.cs
using System;
using System.Text;

namespace Soundex
{
    public static class SoundexAlgorithm
    {
        public static string GetSoundex(string s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            s = s.ToUpper();
            char firstChar = s[0];
            System.Text.StringBuilder soundex = new StringBuilder(firstChar.ToString());
            string soundexCodes = "BFPVCSKGJQXZDTLMNR";
            string soundexValues = "111122222222334556";
            for (int i = 1; i < s.Length; i++)
            {
                int index = soundexCodes.IndexOf(s[i]);
                if (index >= 0)
                {
                    char code = soundexValues[index];
                    if (soundex.Length == 1 || soundex[soundex.Length - 1] != code)
                    {
                        soundex.Append(code);
                    }
                }
            }
            if (soundex.Length > 4)
            {
                soundex.Length = 4;
            }
            else
            {
                while (soundex.Length < 4)
                {
                    soundex.Append('0');
                }
            }
            return soundex.ToString();
        }
    }
}