package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/tjstebbing/piperdb/pkg/db"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	
	// Open database
	cfg := db.DefaultConfig()
	if dir := os.Getenv("PIPERDB_DATA_DIR"); dir != "" {
		cfg.DataDir = dir
	} else {
		cfg.DataDir = "./data"
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}
	
	database, err := db.Open(cfg)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	switch command {
	case "create-list":
		if len(os.Args) < 3 {
			fmt.Println("Usage: piperdb create-list <list-id>")
			os.Exit(1)
		}
		createList(ctx, database, os.Args[2])

	case "add-item":
		if len(os.Args) < 4 {
			fmt.Println("Usage: piperdb add-item <list-id> <json-data>")
			os.Exit(1)
		}
		addItem(ctx, database, os.Args[2], os.Args[3])

	case "list-items":
		if len(os.Args) < 3 {
			fmt.Println("Usage: piperdb list-items <list-id>")
			os.Exit(1)
		}
		listItems(ctx, database, os.Args[2])

	case "show-schema":
		if len(os.Args) < 3 {
			fmt.Println("Usage: piperdb show-schema <list-id>")
			os.Exit(1)
		}
		showSchema(ctx, database, os.Args[2])

	case "list-all":
		listAllLists(ctx, database)

	case "stats":
		if len(os.Args) < 3 {
			fmt.Println("Usage: piperdb stats <list-id>")
			os.Exit(1)
		}
		showStats(ctx, database, os.Args[2])

	case "query":
		if len(os.Args) < 4 {
			fmt.Println("Usage: piperdb query <list-id> <pipe-expression>")
			os.Exit(1)
		}
		queryList(ctx, database, os.Args[2], os.Args[3])

	case "validate":
		if len(os.Args) < 3 {
			fmt.Println("Usage: piperdb validate <pipe-expression>")
			os.Exit(1)
		}
		validatePipe(ctx, database, os.Args[2])

	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("PiperDB CLI Tool")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  piperdb create-list <list-id>")
	fmt.Println("  piperdb add-item <list-id> <json-data>")
	fmt.Println("  piperdb list-items <list-id>")
	fmt.Println("  piperdb show-schema <list-id>")
	fmt.Println("  piperdb list-all")
	fmt.Println("  piperdb stats <list-id>")
	fmt.Println("  piperdb query <list-id> <pipe-expression>")
	fmt.Println("  piperdb validate <pipe-expression>")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println(`  piperdb create-list products`)
	fmt.Println(`  piperdb add-item products '{"name":"iPhone","price":999}'`)
	fmt.Println(`  piperdb list-items products`)
	fmt.Println(`  piperdb query products '@price<1000 | sort -price'`)
	fmt.Println(`  piperdb validate '@category=electronics | count'`)
}

func createList(ctx context.Context, db db.PiperDB, listID string) {
	err := db.CreateList(ctx, listID)
	if err != nil {
		log.Fatalf("Failed to create list: %v", err)
	}
	fmt.Printf("Created list: %s\n", listID)
}

func addItem(ctx context.Context, db db.PiperDB, listID, jsonData string) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		log.Fatalf("Invalid JSON data: %v", err)
	}

	itemID, err := db.AddItem(ctx, listID, data)
	if err != nil {
		log.Fatalf("Failed to add item: %v", err)
	}
	
	fmt.Printf("Added item %s to list %s\n", itemID, listID)
}

func listItems(ctx context.Context, db db.PiperDB, listID string) {
	result, err := db.GetItems(ctx, listID, nil)
	if err != nil {
		log.Fatalf("Failed to get items: %v", err)
	}

	fmt.Printf("Items in list %s (%d total):\n", listID, result.TotalCount)
	for i, item := range result.Items {
		itemJSON, _ := json.MarshalIndent(item, "  ", "  ")
		fmt.Printf("%d. %s\n", i+1, string(itemJSON))
	}
}

func showSchema(ctx context.Context, db db.PiperDB, listID string) {
	schema, err := db.GetSchema(ctx, listID)
	if err != nil {
		log.Fatalf("Failed to get schema: %v", err)
	}

	fmt.Printf("Schema for list %s (version %d):\n", listID, schema.Version)
	schemaJSON, _ := json.MarshalIndent(schema, "  ", "  ")
	fmt.Println(string(schemaJSON))
}

func listAllLists(ctx context.Context, db db.PiperDB) {
	lists, err := db.ListAllLists(ctx)
	if err != nil {
		log.Fatalf("Failed to list all lists: %v", err)
	}

	fmt.Printf("All lists (%d total):\n", len(lists))
	for _, listID := range lists {
		fmt.Printf("  - %s\n", listID)
	}
}

func showStats(ctx context.Context, db db.PiperDB, listID string) {
	stats, err := db.GetStats(ctx, listID)
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}

	fmt.Printf("Statistics for list %s:\n", listID)
	fmt.Printf("  Items: %d\n", stats.ItemCount)
	fmt.Printf("  Size: %d bytes\n", stats.TotalSize)
	fmt.Printf("  Unique fields: %d\n", stats.UniqueFields)
	fmt.Printf("  Last modified: %v\n", stats.LastModified)
}

func queryList(ctx context.Context, db db.PiperDB, listID, pipeExpr string) {
	result, err := db.ExecutePipe(ctx, listID, pipeExpr, nil)
	if err != nil {
		log.Fatalf("Failed to execute pipe: %v", err)
	}

	fmt.Printf("Query: %s\n", pipeExpr)
	fmt.Printf("Results (%d items, took %v):\n", len(result.Items), result.QueryTime)
	
	for i, item := range result.Items {
		itemJSON, _ := json.MarshalIndent(item, "  ", "  ")
		fmt.Printf("%d. %s\n", i+1, string(itemJSON))
	}
}

func validatePipe(ctx context.Context, db db.PiperDB, pipeExpr string) {
	err := db.ValidatePipe(ctx, pipeExpr)
	if err != nil {
		fmt.Printf("Invalid pipe expression: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("Valid pipe expression: %s\n", pipeExpr)
}
