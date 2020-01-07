using Com.ACBC.Framework.Database;
using Newtonsoft.Json;
using QuartzRedis.Common;
using QuartzRedis.Dao;
using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using static QuartzRedis.Dao.SqlDao;

namespace QuartzRedis.Buss
{
    public class TaskJobBuss
    {
        public void doWork(string ids)
        {
            HandleAccount();
        }

        public void HandleAccount( )
        {
            SqlDao sqlDao = new SqlDao();
            sqlDao.getAccountSelectList();
        }

    }
}
