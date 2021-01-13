package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"stock/models"
	"strconv"
	"strings"

	bt "github.com/brkelkar/common_utils/batch"
	db "github.com/brkelkar/common_utils/databases"
	"github.com/brkelkar/common_utils/logger"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

var (
	colMap  map[string]int
	colName = []string{"USERID", "PRODUCTCODE", "CLOSING"}
	err     error
)

func init() {
	colMap = make(map[string]int)
	for _, val := range colName {
		colMap[val] = -1
	}

	// Creating a connection to the database
	dbConfig := db.BuildDBMsSQLConfig("35.200.178.187",
		1433,
		"sqlserver",
		"SmartStockist",
		"test",
	)
	db.DB = make(map[string]*gorm.DB)
	db.DB["SmartStockist"], err = gorm.Open(sqlserver.Open(db.DbMsSQLURL(dbConfig)), &gorm.Config{})
	db.DB["SmartStockist"].AutoMigrate()

	if err != nil {
		logger.Error("Error while connecting to db", err)
		log.Print(err)
	}
}

//StockUpload cloud funtion to upload file
//func StockUpload(ctx context.Context, e models.GCSEvent) error {
func main() {
	//meta, err := metadata.FromContext(ctx)
	//if err != nil {
	//	return fmt.Errorf("metadata.FromContext: %v", err)
	//}
	//log.Printf("Event ID: %v\n", meta.EventID)
	//log.Printf("Event type: %v\n", meta.EventType)

	// Get storage client
	//client, err := storage.NewClient(ctx)
	//if err != nil {
	//	log.Print(err)
	//}

	//filePath := e.Bucket + "/" + e.Name

	filePath := "C:/Users/Rohan Kale/Downloads/11-01-2021_AIOCD0021_Stock_181939c4-94b4-4820-8d66-b725ff0b962f_11-01-2021_e0366956-6e05-446d-9725-b2c574954377.txt"
	rc, err := os.Open(filePath)
	if err != nil {
		log.Print(err)
		//return err
	}

	userID := strings.Split(filePath, "_")[1]

	// Get file reader
	//rc, err := client.Bucket(e.Bucket).Object(e.Name).NewReader(ctx)
	//if err != nil {
	//	log.Print(err)
	//	return err
	//}

	reader := csv.NewReader(rc)
	reader.Comma = '|'
	flag := 1

	var stock []models.Stocks
	productMap := make(map[string]models.Stocks)

	for {
		fileRow, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println(err)
		}
		var tempStock models.Stocks
		var strproductCode string

		for i, val := range fileRow {
			if flag == 1 {
				colMap[strings.ToUpper(val)] = i
			} else {
				switch i {
				case -1:
					break
				case colMap["PRODUCTCODE"]:
					strproductCode = val
					tempStock.ProductCode = val
				case colMap["CLOSING"]:
					if s, err := strconv.ParseFloat(val, 64); err == nil {
						tempStock.Closing = s
					}
				}
				tempStock.UserId = userID
			}
		}

		if flag == 0 {
			val, ok := productMap[strproductCode]
			if ok == true {
				val.Closing = val.Closing + tempStock.Closing
				productMap[strproductCode] = val
			} else {
				productMap[strproductCode] = tempStock
			}
		}
		flag = 0
	}

	for _, val := range productMap {
		stock = append(stock, val)
	}

	db.DB["SmartStockist"].AutoMigrate(&models.Stocks{})
	totalRecordCount := len(stock)
	batchSize := bt.GetBatchSize(stock[0])
	if totalRecordCount <= batchSize {
		db.DB["SmartStockist"].Save(stock)
	} else {
		remainingRecords := totalRecordCount
		updateRecordLastIndex := batchSize
		startIndex := 0
		for {
			if remainingRecords < 1 {
				break
			}
			updateStockBatch := stock[startIndex:updateRecordLastIndex]
			db.DB["SmartStockist"].Save(updateStockBatch)
			remainingRecords = remainingRecords - batchSize
			startIndex = updateRecordLastIndex
			if remainingRecords < batchSize {
				updateRecordLastIndex = updateRecordLastIndex + remainingRecords
			} else {
				updateRecordLastIndex = updateRecordLastIndex + batchSize
			}
		}
	}

	jsonValue, _ := json.Marshal(stock)
	fmt.Println(bytes.NewBuffer(jsonValue))
	//resp, err := http.Post("http://restapi3.apiary.io/notes", "application/json", bytes.NewBuffer(git ))

	//if resp.Status=="200"{

	//}
	//return nil
}
