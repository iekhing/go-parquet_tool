package main

import (
	"fmt"
	"go-parquet_tool/schematool"
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	fr, err := local.NewLocalFileReader("./sampleData/part-00189-02a051d7-0f16-4d35-9fa6-ec890ab1bd81-c000.snappy.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	//columnNum := pr.SchemaHandler.GetColumnNum()
	//log.Println(columnNum)

	// for i := 0; int64(i) < columnNum; i++ {
	// 	exColumnName := pr.SchemaHandler.GetExName(i)
	// 	inColumnName := pr.SchemaHandler.GetInName(i)

	// 	log.Println(exColumnName)
	// 	log.Println(inColumnName)
	// }

	schemaTree := schematool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)

	fmt.Printf("%s", schemaTree.Root.OutputJsonSchema())

	// num := int(pr.GetNumRows())

	// values, _, _, err := pr.ReadColumnByPath("spark_schema.ifa", int64(num))
	// if err != nil {
	// 	log.Println("Can't read column", err)
	// 	return
	// }

	// for _, value := range values {
	// 	println(fmt.Sprintf("%v", value))
	// }

	// if err != nil {
	// 	log.Println("Can't read", err)
	// 	return
	// }

	//log.Println(num)

	pr.ReadStop()
	fr.Close()
}
