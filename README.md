# Gateway Failover（初版）

依「[Gateway Failover 設計規格](docs/SPEC.md)」實作的 C++20 初版（文中的「§」皆指該規格的章節）。單執行緒、poll 驅動，所有時間來自 `Clock`，
測試用模擬時鐘與 in-process Mock Broker，結果可重現。

## 建置與測試

```bash
cmake -S . -B build -G Ninja && cmake --build build
./build/gw_tests            # 全部測試（預設開 ASan/UBSan）
./build/gw_tests "9.3"      # 只跑名稱含 "9.3" 的測試
./build/killswitch --account ACC1 --demo
```

關閉 sanitizer：`cmake -S . -B build -DGW_SANITIZE=OFF`。無外部相依（測試框架自帶於 `tests/testing.hpp`）。

## 元件對照（規格第 4 節）

| 元件 | 檔案 | 規格 |
|---|---|---|
| 資料模型 | `include/gw/types.hpp` | §6、§7 |
| ClOrdID 編碼 | `clordid.hpp/.cpp` | §5 |
| Order State Machine | `order_book.hpp/.cpp` | §7 |
| WAL | `wal.hpp/.cpp` | §6 |
| Broker 介面 | `broker.hpp` | §3、§16 |
| Mock Broker | `mock_broker.hpp/.cpp` | §14 |
| Reconciler | `reconciler.hpp/.cpp` | §9 |
| Connection Manager + 恢復模式決策 | `connection.hpp/.cpp` | §10 |
| Gateway（重啟、對帳處理、全撤單） | `gateway.hpp/.cpp` | §8、§9.3、§11 |
| 策略通道（in-process） | `strategy_channel.hpp` | §8 step 8 |
| Kill flag / 心跳 | `safety.hpp/.cpp` | §12.2、§13 |
| Kill Switch + Watchdog | `kill_switch.hpp/.cpp`、`tools/killswitch_main.cpp` | §12、§13 |

## 規格未明講、我自己做的決定

- **ClOrdID 格式**：`GG` 兩位 gateway 編號 + `CCC` 三碼策略短碼 + `NNNNNNN` 七位流水號，共 12 字元（如 `01MMK0000042`）。撤單/改單的 request id 用同一個流水號序列。
- **改單模型**：一筆委託在整個生命週期都用原始 ClOrdID 識別，撤單/改單另帶 `request_id`。FIX 的 OrigClOrdID 鏈要在 adapter 裡轉成這個模型。
- **狀態是事實的純函數**：訂單只累積事實（acked、canceled、rejected、成交集合、撤銷量、最大改單序號），狀態由這些事實推導，所以套用順序無關、重複套用無害。有性質測試驗證。
- **沒有成交序號的成交**：合成鍵 =（委託書號、價、量、時間）+ 出現次數，次數按「串流」分開算（即時、回補各自計數），兩條串流描述同一批事實時會自然去重。限制：同一條串流內，券商真的重送同一筆合成鍵的成交時無法分辨，會多算一次。
- **回補期間的即時回報**只進緩衝區、不寫 WAL；對帳後以 `recovery` 來源把差異事實寫回 WAL。
- **對帳修正寫成合成回報**（source `recovery`），成交的 exec_id 填入已解析的鍵，重複匯入不會重複計算。
- **3a/3b/5** 都是「第一輪先記下 → 等 `recheck_delay` → 第二輪重新回補」；第二輪仍不符才判定失敗（3a）或停止交易（3b/5）。
- **乾淨與否**：3b、5、6、7、帳戶部位不符、部位查詢失敗都算不乾淨；4b（孤兒單，會撤單並告警）和 4c（外部單）不影響乾淨判定。
- **停止交易是持久的**：`AccountHalt` 寫進 WAL，重啟後仍維持停止，必須 `operator_release()`。
- **全撤單絕不碰外部通路的單**（4c），包括帳號層級批次撤單的範圍判斷。
- **重啟時的斷線時間** = 現在 − WAL 最後一筆紀錄時間（空 WAL 視為 0）。
- **價格變動條件**：有接 `MarketData` 才啟用；拿不到斷線前或目前價格時視為超過門檻（保守）。斷線前價格取的是「最後一次連線正常時的 poll」。
- **長時間斷線（≥60 秒）**：所有策略進入 halt，等人工放行；帳號本身不標停止。
- **回補重試**：間隔 `replay_retry_delay × 第 n 次`，超過 `replay_max_retries` 就改用查詢 API 並停止交易。
- **Kill switch 預設只撤我方通路（API）的單**，`include_other_channels` 可改。連續兩次查詢為空才算完成；每筆撤單最多試 `max_attempts_per_order` 次，之後列為殘留並告警。

## 測試涵蓋（42 項）

規格 §14 必測 10 項全部有對應測試（名稱以 `#1`…`#10` 開頭），§9.3 每種比對結果各一項（`9.3 #…`），
另有：狀態機性質測試、WAL 往返/殘缺/檢查碼、重連時間表與端點輪替、抖動、登入被拒、
恢復模式決策、價格變動升級、策略心跳遺失、撤單被拒、watchdog、kill switch 範圍與卡住情況。

## 初版沒做 / 已知限制

- 真實券商 adapter、自動平倉、跨機 WAL 複寫（只有 `WalReplicator` 介面）。
- WAL 檢查點（`RecordType::Checkpoint` 已保留）。目前每次重啟從頭重播，WAL 一天一個檔。
- 一個 gateway 只管一個帳號。
- 券商故障偵測只做了「短時間大量拒單」；回報停止、延遲暴增還沒做。
- 改單沒有序號也沒有時間時只標記 `amend_uncertain`，還沒接查詢 API 取當前值。
- 3a 遇到「查詢看得到、回補看不到」時直接停止交易，沒有更細的處理。
- 收盤時間判斷只留了 `market_open` 回呼，重連不會在收盤後自動停。
- 「已推送給策略」的判斷以 WAL 為準：對帳修正寫入 WAL 後、推送前若又崩潰，那幾筆成交不會再推一次（快照仍包含完整部位與在途單）。

## 仍需你決定的事項（規格 §16）

- 券商實際能力：批次撤單、斷線刪單、第二組憑證、跨 session 撤單、回補是否有結束標記與筆數。目前都是 `BrokerCapabilities` 旗標，Mock 可切換。
- 時間與價格門檻的實際數值：現在用規格的預設值（10s/60s/30s、5s、0.5%、3 次/10 分鐘），都可在 `ReconnectPolicy`、`RecoveryPolicy`、`GatewayConfig` 設定。
- 撤單驗證間隔、每筆撤單重試次數、回補靜默逾時：現為 500ms、3 次、2s。
