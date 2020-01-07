using Com.ACBC.Framework.Database;
using QuartzRedis.Buss;
using QuartzRedis.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace QuartzRedis.Dao
{
    class SqlDao
    {
        public void getAccountSelectList()
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendFormat(MemberSqls.SELECT_MAX_ACCOUNT_DAY);
            string sql = builder.ToString();
            DataTable dt = DatabaseOperationWeb.ExecuteSelectDS(sql, "T").Tables[0];
            if (dt != null && dt.Rows.Count > 0)
            {
                double proportion = getProportion();
                DateTime maxDate = Convert.ToDateTime(dt.Rows[0][0]).AddDays(1);
                for (DateTime i = maxDate; i < DateTime.Now.AddDays(-1); i = i.AddDays(1))
                {
                    HandleAccountPhone(Convert.ToDateTime("2019-12-01"));
                    ArrayList al = new ArrayList();
                    string dateStr = i.ToString("yyyy-MM-dd");
                    StringBuilder builder1 = new StringBuilder();
                    builder1.AppendFormat(MemberSqls.SELECT_LEEK_PHONE_BY_DATE, i.AddDays(1).ToString("yyyy-MM-dd"));
                    string sql1 = builder1.ToString();
                    DataTable dt1 = DatabaseOperationWeb.ExecuteSelectDS(sql1, "T").Tables[0];
                    if (dt1 != null && dt1.Rows.Count > 0)
                    {
                        string phone = "";
                        foreach (DataRow dr in dt1.Rows)
                        {
                            if (phone == "")
                            {
                                phone = "'" + dr["phone"].ToString() + "'";
                            }
                            else
                            {
                                phone += ",'" + dr["phone"].ToString() + "'";
                            }
                        }
                        try
                        {
                            DatabaseOperationWeb.TYPE = new DBManagerZE();
                            StringBuilder builder2 = new StringBuilder();
                            builder2.AppendFormat(MemberSqls.SELECT_RECHARGE_LIST_BY_PHONES, dateStr, phone);
                            string sql2 = builder2.ToString();
                            DataTable dt2 = DatabaseOperationWeb.ExecuteSelectDS(sql2, "T").Tables[0];
                            if (dt2 != null && dt2.Rows.Count > 0)
                            {
                                foreach (DataRow dr2 in dt2.Rows)
                                {
                                    DataRow[] drs = dt1.Select("phone='" + dr2["chat_user_id"].ToString() + "'");
                                    if (drs.Length > 0)
                                    {
                                        double pro = proportion;
                                        double.TryParse(drs[0]["proportion"].ToString(), out pro);
                                        double price = Convert.ToDouble(dr2["ALLREC_MONEY"]) * pro;
                                        StringBuilder builder3 = new StringBuilder();
                                        builder3.AppendFormat(MemberSqls.ADD_ACCOUNT, dateStr, drs[0]["LEEK_MEMBER_ID"].ToString(),
                                            drs[0]["LEEK_NAME"].ToString(), drs[0]["PHONE"].ToString(),
                                            Convert.ToDouble(dr2["ALLREC_MONEY"]), price.ToString(),
                                            drs[0]["MEMBER_ID"].ToString(), drs[0]["RESELLER_TYPE"].ToString());
                                        string sql3 = builder3.ToString();
                                        al.Add(sql3);
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        { 
                        }
                        finally
                        {
                            DatabaseOperationWeb.TYPE = new DBManager();
                        }
                    }
                    StringBuilder builder4 = new StringBuilder();
                    builder4.AppendFormat(MemberSqls.ADD_ACCOUNT_LOG, al.Count, dateStr);
                    string sql4 = builder4.ToString();
                    al.Add(sql4);
                    DatabaseOperationWeb.ExecuteDML(al);
                }
            }

        }
        public double getProportion()
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendFormat(MemberSqls.SELECT_PROPORTION);
            string sql = builder.ToString();
            DataTable dt = DatabaseOperationWeb.ExecuteSelectDS(sql, "T").Tables[0];
            if (dt != null && dt.Rows.Count > 0)
            {
                return Convert.ToDouble(dt.Rows[0][0]);
            }
            else
            {
                return 0;
            }
        }
        public void HandleAccountPhone(DateTime dateTime)
        {
            ArrayList al = new ArrayList();
            StringBuilder builder1 = new StringBuilder();
            builder1.AppendFormat(MemberSqls.SELECT_LEEK_PHONE_BY_DATE_FLAG, dateTime.ToString("yyyy-MM-dd"));
            string sql1 = builder1.ToString();
            DataTable dt1 = DatabaseOperationWeb.ExecuteSelectDS(sql1, "T").Tables[0];
            if (dt1 != null && dt1.Rows.Count > 0)
            {
                string phone = "";
                foreach (DataRow dr in dt1.Rows)
                {
                    if (phone == "")
                    {
                        phone = "'" + dr["phone"].ToString() + "'";
                    }
                    else
                    {
                        phone += ",'" + dr["phone"].ToString() + "'";
                    }
                }
                if (phone != "")
                {
                    try
                    {
                        DatabaseOperationWeb.TYPE = new DBManagerZE();
                        StringBuilder builder2 = new StringBuilder();
                        builder2.AppendFormat(MemberSqls.SELECT_RECHARGE_LIST_BY_PHONES_3MONTHAGO,
                            dateTime.AddMonths(-3).ToString("yyyy-MM-dd"), dateTime.AddDays(-1).ToString("yyyy-MM-dd"), phone);
                        string sql2 = builder2.ToString();
                        DataTable dt2 = DatabaseOperationWeb.ExecuteSelectDS(sql2, "T").Tables[0];
                        if (dt2 != null && dt2.Rows.Count > 0)
                        {
                            string ids = "";
                            foreach (DataRow dr2 in dt2.Rows)
                            {
                                DataRow[] drs = dt1.Select("phone='" + dr2["chat_user_id"].ToString() + "'");
                                if (drs.Length > 0)
                                {
                                    if (ids == "")
                                    {
                                        ids = "'" + drs[0]["PID"].ToString() + "'";
                                    }
                                    else
                                    {
                                        ids += ",'" + drs[0]["PID"].ToString() + "'";
                                    }
                                }

                            }
                            if (ids != "")
                            {
                                StringBuilder builder3 = new StringBuilder();
                                builder3.AppendFormat(MemberSqls.UPDATE_MEMBER_LEEK_BY_PHONE, ids);
                                string sql3 = builder3.ToString();
                                al.Add(sql3);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                         
                    }
                    finally
                    {
                        DatabaseOperationWeb.TYPE = new DBManager();
                    }
                }
            }
            if (al.Count > 0)
            {
                DatabaseOperationWeb.ExecuteDML(al);
            }
        }
        public class MemberSqls
        {

            public const string SELECT_MAX_ACCOUNT_DAY = ""
                + "SELECT ACCOUNT_DATE " +
                "FROM T_ACCOUNT_LOG " +
                "ORDER BY ID DESC " +
                "LIMIT 1  ";

            public const string SELECT_LEEK_PHONE_BY_DATE = ""
                + "SELECT L.*,P.PHONE,M.* " +
                "FROM T_MEMBER_LEEK L ,T_MEMBER_PHONE P,T_BASE_MEMBER M " +
                "WHERE L.LEEK_MEMBER_ID = P.MEMBER_ID " +
                "AND L.MEMBER_ID = M.MEMBER_ID " +
                "AND PHONE <> ''  " +
                "AND P.FLAG='1' " +
                "AND P.SHOP_TYPE='2' " +
                "AND L.CREATETIME <STR_TO_DATE('{0}', '%Y-%m-%d') ";
            public const string SELECT_RECHARGE_LIST_BY_PHONES = ""
                + "SELECT U.CHAT_USER_ID,C.ALLREC_MONEY/10 as ALLREC_MONEY " +
                "FROM REPORT_RECHARGE_COUNT C ,U_USER U " +
                "WHERE C.USER_ID = U.USER_ID " +
                "AND C.INSERT_DATE = '{0}' " +
                "AND U.CHAT_USER_ID in ({1}) " +
                "AND ALLREC_MONEY >0";
            public const string SELECT_RECHARGE_LIST_BY_PHONES_3MONTHAGO = ""
                + "SELECT U.CHAT_USER_ID " +
                "FROM REPORT_RECHARGE_COUNT C ,U_USER U " +
                "WHERE C.USER_ID = U.USER_ID " +
                "AND C.INSERT_DATE BETWEEN '{0}' and '{1}' " +
                "AND U.CHAT_USER_ID in ({2}) " +
                "AND ALLREC_MONEY >0 " +
                "GROUP BY U.CHAT_USER_ID";

            public const string ADD_ACCOUNT_LOG = ""
                + "INSERT INTO T_ACCOUNT_LOG(ACCOUNT_COUNT,ACCOUNT_DATE,CREATETIME) "
                + "VALUES('{0}','{1}',NOW())";
            public const string ADD_ACCOUNT = ""
                + "INSERT INTO T_ACCOUNT_LIST(ACOUNT_DATE,MEMBER_ID,MEMBER_NAME,PHONE,ACOUNT_PRICE,RESELLER_PRICE,"
                + "CREATETIME,RESELLER_MEMBER_ID,RESELLER_TYPE,STATE) "
                + "VALUES('{0}',{1},'{2}','{3}',{4},{5},NOW(),'{6}','{7}','0')";

            public const string UPDATE_MEMBER_LEEK_BY_PHONE =
                "UPDATE T_MEMBER_PHONE SET FLAG='0' " +
                "WHERE ID IN ({0})   ";
            public const string SELECT_LEEK_PHONE_BY_DATE_FLAG = ""
               + "SELECT L.*,P.ID AS PID,P.PHONE " +
               "FROM T_MEMBER_LEEK L ,T_MEMBER_PHONE P " +
               "WHERE L.LEEK_MEMBER_ID = P.MEMBER_ID " +
               "AND PHONE <> ''  " +
               "AND P.FLAG='1' " +
               "AND P.SHOP_TYPE='2' " +
               "AND DATE_FORMAT(L.CREATETIME,'%Y-%m-%d') = '{0}' ";
            public const string SELECT_PROPORTION = ""
               + "SELECT CONFIG_VALUE FROM T_SYS_CONFIG WHERE CONFIG_CODE='001'";
        }
    }
}
