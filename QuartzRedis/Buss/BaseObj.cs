﻿using System;
using System.Collections.Generic;
using System.Text;

namespace QuartzRedis.Buss
{
    public class ConfigParam
    {
        public string env;
        public string group;
    }
    public class GMember
    {
        public string ME_ID;
        public string ME_MobileNum;
        public string ME_Score;
    }
    public class GGiftSellMasterChangeParam
    {
        public string GT_GUID;
        public string ME_MobileNum;
        public string GTM_Score;
        public string GT_Type;
    }
    public class GRechargeDetailChangeParam
    {
        public string RD_GUID;
        public string ME_MobileNum;
        public string RD_GiveScore;
        public string RD_Type;
    }
    public class GMemberParam
    {
        public string ME_ID;
        public string ME_Name;
        public string ME_Point;
        public string ME_Score;
        public string ME_Coin;
        public string ME_MobileNum;
        public string ME_Ticket;
        public string ME_ShopNo;
        public string ME_F_CT_ID;
    }
    public class PostParam
    {
        public string sign;
        public string code;
        public string nonceStr;
        public string method;
        public string appId;
        public Param param;
    }
    public class Param { }

    public class AddMemberInfoParam : Param
    {
        public string phone;
        public string point;
        public string cardCode;
    }
    public class AddPointRecordParam : Param
    {
        public string phone;
        public string point;
    }
    public class UserPointParam : Param
    {
        public string phone;
        public string point;
        public string cardCode;
        public string totalPoint;
    }
    public class UpdatePointCommitParam : Param
    {
        public string pointCommitId;
    }
    public class ReturnItem
    {
        public bool success;
        public Message msg;
        public List<ReturnData> data;
    }
    public class Message
    {
        public string code;
        public string msg;
    }
    public class ReturnData
    {
        public string pointCommitId;// 变更ID
        public string storeId;//店铺
        public string phone;//电话
        public string memberId;//会员ID（无效）
        public string state;//状态（无效）
        public string type;//类型（无效）
        public string point;//变更积分
    }
}
