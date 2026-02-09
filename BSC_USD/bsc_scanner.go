package BSC_USD

import (
	"fmt"
	"log"
	"math"
	"upay_pro/db/sdb"
	"upay_pro/mylog"

	"github.com/wangegou/bsc-usdt-scanner/scanner"
)

func Start_scan(order sdb.Orders) bool {
	mylog.Logger.Info("正在通过扫区块获取BSC-USD 入账方向的交易数据...")
	// 需要监控的钱包地址
	// walletAddr := "0x5bd808Ab85C124f99080da5F864EDcB39950edE5"

	// 开始扫描 (默认扫描过去 30 个区块，超时时间 1 分钟)
	// 返回是一个按照时间倒序排列的切片

	records, err := scanner.StartScan(order.Token, "USDT")
	if err != nil {
		log.Fatalf("扫描失败: %v", err)
	}

	mylog.Logger.Info(fmt.Sprintf("扫描完成! 发现 %d 笔入账:\n", len(records)))

	// 打印第一条（最新的）记录作为示例
	if len(records) > 0 {
		/* rec := records[0]
		fmt.Printf("最新一笔入账:\n")
		fmt.Printf("- 时间:   %s\n", rec.Time.Format("2006-01-02 15:04:05"))
		fmt.Printf("- 金额:   %f USDT\n", rec.Amount)
		fmt.Printf("- 来自:   %s\n", rec.From)
		fmt.Printf("- 哈希:   https://bscscan.com/tx/%s\n", rec.TxHash) */
		// 只验证最新的一条入账记录
		rec := records[0]

		// 将时间转为毫秒时间戳
		timeStampMs := rec.Time.UnixMilli()
		// 将金额转为float64

		amountFloat64, _ := rec.Amount.Float64()
		// 保留2位小数：先乘以100，四舍五入，再除以100
		roundedAmount := math.Round(amountFloat64*100) / 100
		// 如果查询的记录在订单的开始和过期时间之间，且金额匹配且TxHash不为空，则认为交易成功
		if (order.StartTime < timeStampMs && timeStampMs < order.ExpirationTime) && roundedAmount == order.ActualAmount && rec.TxHash != "" {
			mylog.Logger.Info("BSC-USD 交易记录符合本次交易验证，接下来更新数据库")
			order.BlockTransactionId = rec.TxHash
			order.Status = sdb.StatusPaySuccess
			// 更新数据库订单记录
			re := sdb.DB.Save(&order)
			if re.Error == nil {
				mylog.Logger.Info("USDT_BSC 订单入账成功")
				return true
			}
			mylog.Logger.Error("USDT_BSC 订单入账失败")
			return false
		} else {
			mylog.Logger.Info("BSC-USD 交易记录不符合本次交易验证")
			mylog.Logger.Info(fmt.Sprintf("- 时间:   %s\n", rec.Time.Format("2006-01-02 15:04:05")))
			mylog.Logger.Info(fmt.Sprintf("- 金额:   %f USDT\n", rec.Amount))
			mylog.Logger.Info(fmt.Sprintf("- 来自:   %s\n", rec.From))
			mylog.Logger.Info(fmt.Sprintf("- 哈希:   https://bscscan.com/tx/%s\n", rec.TxHash))

			return false
		}

	}

	return false
}
