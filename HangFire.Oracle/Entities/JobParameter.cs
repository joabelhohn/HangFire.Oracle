﻿namespace Hangfire.Oracle.Entities
{
    internal class JobParameter
    {
        public int JobId { get; set; }
        public string Name { get; set; }
        public string Value { get; set; }
    }
}
