#!/bin/bash

# PiperDB Demo Script
# This script demonstrates the core functionality of PiperDB

set -e

echo "🚀 PiperDB Demo"
echo "================="

# Set up paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PIPERDB="$ROOT_DIR/piperdb"

# Build if needed
if [ ! -f "$PIPERDB" ]; then
    echo "Building piperdb..."
    cd "$ROOT_DIR" && go build ./cmd/piperdb
fi

# Clean up any existing data
rm -rf "$SCRIPT_DIR/demo-data"
mkdir -p "$SCRIPT_DIR/demo-data"

# Set data directory for demo
export PIPERDB_DATA_DIR="$SCRIPT_DIR/demo-data"

echo ""
echo "📋 Creating lists and adding data..."

# Create product catalog
"$PIPERDB" create-list products

# Add sample products
"$PIPERDB" add-item products '{"name":"iPhone 15 Pro","price":999,"brand":"Apple","category":"smartphone","rating":4.8,"stock":25}'
"$PIPERDB" add-item products '{"name":"MacBook Pro","price":2499,"brand":"Apple","category":"laptop","rating":4.9,"stock":15}'
"$PIPERDB" add-item products '{"name":"Pixel 8","price":699,"brand":"Google","category":"smartphone","rating":4.6,"stock":30}'
"$PIPERDB" add-item products '{"name":"Surface Pro","price":1299,"brand":"Microsoft","category":"laptop","rating":4.3,"stock":20}'
"$PIPERDB" add-item products '{"name":"Galaxy S24","price":899,"brand":"Samsung","category":"smartphone","rating":4.7,"stock":35}'
"$PIPERDB" add-item products '{"name":"ThinkPad X1","price":1899,"brand":"Lenovo","category":"laptop","rating":4.5,"stock":12}'

echo "✅ Added 6 products to catalog"

# Create articles
"$PIPERDB" create-list articles

# Add sample articles
"$PIPERDB" add-item articles '{"title":"Getting Started with Go","author":"Alice","status":"published","tags":["golang","tutorial"],"views":1542,"date":"2024-01-15"}'
"$PIPERDB" add-item articles '{"title":"Database Optimization Tips","author":"Bob","status":"published","tags":["database","performance"],"views":892,"date":"2024-01-10"}'
"$PIPERDB" add-item articles '{"title":"Modern Web Development","author":"Carol","status":"draft","tags":["web","javascript"],"views":0,"date":"2024-01-20"}'
"$PIPERDB" add-item articles '{"title":"Go vs Rust Performance","author":"Alice","status":"published","tags":["golang","rust","performance"],"views":2341,"date":"2024-01-08"}'

echo "✅ Added 4 articles"

echo ""
echo "🔍 Basic Filtering Examples"
echo "============================"

echo ""
echo "1. Find products under $1000:"
"$PIPERDB" query products '@price:<1000'

echo ""
echo "2. Find Apple products:"
"$PIPERDB" query products '@brand:Apple'

echo ""
echo "3. Find smartphones with rating >= 4.5:"
"$PIPERDB" query products '@category:smartphone @rating:>=4.5'

echo ""
echo "📊 Sorting and Selection Examples"
echo "================================="

echo ""
echo "1. Top 3 products by price:"
"$PIPERDB" query products 'sort -price | take 3'

echo ""
echo "2. Products by category and price (name and price only):"
"$PIPERDB" query products 'sort category -price | select name price category'

echo ""
echo "3. Cheapest smartphone:"
"$PIPERDB" query products '@category:smartphone | sort price | first'

echo ""
echo "🔢 Aggregation Examples"
echo "======================="

echo ""
echo "1. Count products by brand:"
echo "Apple:"
"$PIPERDB" query products '@brand:Apple | count'
echo "Google:" 
"$PIPERDB" query products '@brand:Google | count'
echo "Samsung:"
"$PIPERDB" query products '@brand:Samsung | count'

echo ""
echo "2. Average price of all products:"
"$PIPERDB" query products 'avg price'

echo ""
echo "3. Total inventory value:"
"$PIPERDB" query products 'sum price'

echo ""
echo "📰 Content Management Examples"
echo "=============================="

echo ""
echo "1. Published articles by views:"
"$PIPERDB" query articles '@status:published | sort -views'

echo ""
echo "2. Articles by Alice:"
"$PIPERDB" query articles '@author:Alice'

echo ""
echo "3. Articles containing 'Go':"
"$PIPERDB" query articles '"Go"'

echo ""
echo "🔄 Transformation Examples"  
echo "=========================="

echo ""
echo "1. Product summary (rename fields):"
"$PIPERDB" query products 'map {name, price: cost, brand: manufacturer} | take 3'

echo ""
echo "2. Article metadata only:"
"$PIPERDB" query articles '@status:published | select title author views'

echo ""
echo "📈 Complex Pipeline Examples"
echo "============================"

echo ""
echo "1. E-commerce analytics:"
"$PIPERDB" query products '@rating:>=4.5 @price:>500 | sort -rating -price | select name price rating brand | take 3'

echo ""
echo "2. Content performance analysis:"
"$PIPERDB" query articles '@status:published | sort -views | map {title, author, performance: views} | take 3'

echo ""
echo "🎯 Schema Information"
echo "===================="

echo ""
echo "Products schema:"
"$PIPERDB" show-schema products

echo ""
echo "Articles schema:"
"$PIPERDB" show-schema articles

echo ""
echo "📊 Performance Statistics"
echo "========================="

echo ""
echo "Products stats:"
"$PIPERDB" stats products

echo ""
echo "Articles stats:"  
"$PIPERDB" stats articles

echo ""
echo "✅ Demo completed!"
echo ""
echo "🎉 PiperDB demonstrates:"
echo "   • Flexible list storage with automatic schema detection"
echo "   • Powerful pipe-based query language"
echo "   • Sub-millisecond query performance"
echo "   • Rich filtering, sorting, and aggregation capabilities"
echo ""
echo "Try your own queries with: "$PIPERDB" query <list-name> '<pipe-expression>'"
echo "Example: "$PIPERDB" query products '@price:<800 @rating:>4 | sort -rating | take 5'"
