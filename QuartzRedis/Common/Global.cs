using System;
using System.Collections.Generic;
using System.Text;

namespace QuartzRedis.Common
{
    public class Global
    {
        public const string ENV = "PRO";
        public const string GROUP = "Task";

        public const string TASK_JOB = "GIFT-ZHUAE-USER";

        public const string TASK_PREFIX = "Task";

        public const string CONFIG_TOPIC = "ConfigServerTopic";
        public const string TASK_TOPIC = "TaskTopic";

        public const string TOPIC_MESSAGE = "update";
        public const int REDIS_DB = 11;

        public static string Redis
        {
            get
            {
                return Environment.GetEnvironmentVariable("Redis");
            }
        }
        public static string AppId
        {
            get
            {
                return Environment.GetEnvironmentVariable("AppId");
            }
        }
        public static string AppSecret
        {
            get
            {
                return Environment.GetEnvironmentVariable("AppSecret");
            }
        }
        public static string PlaceHold
        {
            get
            {
                return Environment.GetEnvironmentVariable("PlaceHold");
            }
        }
        public static string PostUrl
        {
            get
            {
                return Environment.GetEnvironmentVariable("PostUrl");
            }
        }


    }
}
