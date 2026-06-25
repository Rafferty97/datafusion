// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! See `main.rs` for how to run it.

use std::io::Write;

use arrow::array::StringArray;
use datafusion::{
    error::Result,
    execution::{context::SessionContext, options::CsvReadOptions},
};

/// This example demonstrates...
pub async fn non_utf8_csv() -> Result<()> {
    // Encode a test CSV into SHIFT-JIS
    let data = r#"ID,Name,Price,Description,Notes
001,山本 大輔,\2945,桜餅と抹茶のセット,数量限定
002,加藤 由美,\9575,和牛ステーキセット,取り寄せ中
003,田中 太郎,\1853,抹茶アイスクリーム,ポイント2倍
004,渡辺 さくら,\9494,和牛ステーキセット,送料無料
005,加藤 由美,\558,和牛ステーキセット,新商品
006,渡辺 さくら,\7704,天ぷら盛り合わせ,割引対象外
007,田中 太郎,\212,桜餅と抹茶のセット,取り寄せ中
008,中村 陽子,\8847,和牛ステーキセット,期間限定
009,伊藤 健太,\5997,季節の野菜カレー,お一人様1点限り
010,高橋 美咲,\6594,季節の野菜カレー,冷凍保存"#;
    let (data, _, _) = encoding_rs::SHIFT_JIS.encode(data);

    // Write the CSV data to a temp file
    let mut tmp = tempfile::Builder::new().suffix(".csv").tempfile()?;
    tmp.write_all(&*data)?;
    let path = tmp.path().to_str().unwrap().to_string();

    // Read the file
    let ctx = SessionContext::new();
    let opts = CsvReadOptions::new().has_header(true); //.charset("SHIFT-JIS");
    let batches = ctx.read_csv(path, opts).await?.collect().await?;

    // Check
    let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(num_rows, 10);

    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "山本 大輔");
    assert_eq!(names.value(1), "加藤 由美");
    assert_eq!(names.value(2), "田中 太郎");

    Ok(())
}
