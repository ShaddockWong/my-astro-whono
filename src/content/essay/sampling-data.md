---
title: 一次基于 SQL 的智能质检抽样重构记
description: 从模糊直觉到严谨指令
date: 2026-03-19
tags: ["SQL", "代码", "思考"]
draft: false
---

> “很多时候，我们以为自己理解了一段逻辑，直到试图把它重构。”

最近接到一个需求：在不同的业务场景下，需要根据质检得分动态抽取数据进行人工复核。由于平台侧无法实现这种高度定制化的逻辑，任务落到了我们头上——我们需要自己写一套抽样引擎。

起初，我的脑海里只有模糊的直觉：*“先算分，再打标，最后随机抽几个不就行了？”* 但真当我要把这句话翻译成代码时，却发现逻辑千头万绪：不同场景的阈值不同、抽样比例各异、还要保证随机性的公平……

苦思冥想许久，代码在脑子里打结。直到我尝试让 AI 协助梳理思路，那一刻瞬间茅塞顿开。**原来，编程的本质就是逼迫你把那些“大概也许”的直觉，变成编译器无法容忍歧义的严谨指令。**

今天记录下这次从“想不出来”到“豁然开朗”的重构过程，以及最终落地的 SQL 方案。

## 一、核心思路：秩序的建立

面对混乱的业务规则，我们不能直接跳进细节，而要先建立秩序。整个逻辑被拆解为六个严密的步骤，环环相扣：

1.  **数据聚合（Aggregation）**：基于业务强约束（单订单单场景），将细粒度的得分汇总。
2.  **规则判定（Rule Engine）**：将业务语言翻译为 `CASE WHEN`，生成明确的复核标签（如“必检”、“免检”、“抽5%”）。
3.  **抽样配置（Configuration）**：按标签分组，计算每组具体需要抽取的数量（注意处理 `CEIL` 向上取整和最小值为1的逻辑）。
4.  **构建随机池（Randomization）**：打乱数据顺序，为每条数据分配一个随机序号。
5.  **截断输出（Filtering）**：根据计算出的数量，截取序号靠前的数据。
6.  **最终交付**：输出确定的复核名单。

这个过程本身就是一种残酷的思考训练：**编译器不会容忍含糊其辞，每一个变量都需要被定义，每一个分支都需要被覆盖。**

## 二、代码实现：诚实的 SQL

代码是诚实的。内存不会撒谎，日志不会遗忘。以下是经过精简和优化后的最终版 SQL。

> **💡 脱敏说明**：以下代码已对表名、字段名及具体业务数值进行了通用化脱敏处理，保留了核心逻辑结构，可直接作为模板参考。

```sql
/*
 * ==============================================================================
 * 脚本名称：智能质检 (AI QC) 自动抽样复核名单生成器
 * 版本：Final Refactored v1.0
 * 
 * [设计哲学]
 *   - 秩序优于混乱：通过 CTE 将复杂的业务逻辑拆解为线性的数据处理流。
 *   - 诚实优于掩饰：利用数据库特性直接表达业务约束，不写防御性冗余代码。
 *
 * [前提约束]
 *   - 业务强约束：同一订单 (order_id) 下，场景 (scene) 字段唯一，无混合场景。
 *   - 数据时效：仅处理最近 N 天的数据。
 * ==============================================================================
 */

WITH
-- 1. [数据源] 提取最近 N 天的原始明细数据
-- 思考：这里定义了时间的边界，是秩序的起点。
raw_data AS (
    SELECT *
    FROM dwd.dwd_store_ai_qc_info -- [脱敏] 原始宽表
    WHERE start_time >= date_format(current_date - 2, 'YYYY-MM-DD 00:00:00')
),

-- 2. [核心聚合] 利用“单段单场景”特性进行降维
-- 重构点：之前纠结于复杂的优先级判断，其实既然场景唯一，直接 GROUP BY 即可。
agg_final AS (
    SELECT 
        date(start_time) as stat_date,
        order_id,                -- [脱敏] 原 qc_id
        order_no,                -- [脱敏] 原 qc_order
        MIN(start_time) as start_time,

        -- [逻辑映射] 将底层场景码映射为标准业务类型
        CASE
            WHEN MAX(scene_code) = 'sensitive_words' THEN 'SENSITIVE_WORDS'
            WHEN MAX(scene_code) IN ('seven_steps', 'three_forbid') THEN 'STORE_SERVICE'
            WHEN MAX(scene_code) = 'change_buy' THEN 'CHANGE_BUY'
            WHEN MAX(scene_code) = 'recommendation' THEN 'RECOMMENDATION'
            ELSE 'UNKNOWN'
        END AS final_scene_type,

        -- [分值汇总] 直接求和即为该场景总分
        SUM(score_value) AS final_score

    FROM raw_data
    GROUP BY date(start_time), order_id, order_no
),

-- 3. [规则引擎] 生成复核标签
-- 思考：这里是业务逻辑最密集的地方，必须穷尽所有分支，不能有“大概”。
rule_final AS (
    SELECT 
        stat_date,
        order_id,
        order_no,
        start_time,
        final_scene_type,
        final_score,
        CASE
            -- 场景 A：敏感词 (0分必检，否则免检)
            WHEN final_scene_type = 'SENSITIVE_WORDS' THEN
                CASE WHEN final_score = 0 THEN 'MUST_CHECK_SENS' ELSE 'AUTO_PASS_SENS' END

            -- 场景 B：门店服务 (分级策略：高分免检，中分低比例抽，低分必检)
            WHEN final_scene_type = 'STORE_SERVICE' THEN
                CASE
                    WHEN final_score >= 180 THEN 'AUTO_PASS_STORE'
                    WHEN final_score >= 160 THEN 'SAMPLE_5PCT_STORE'
                    ELSE 'MUST_CHECK_STORE'
                END

            -- 场景 C & D：换购/推荐 (有分即抽，无分免检)
            WHEN final_scene_type IN ('CHANGE_BUY', 'RECOMMENDATION') THEN
                CASE WHEN final_score > 0 THEN 'SAMPLE_10PCT_GENERIC' ELSE 'AUTO_PASS_GENERIC' END

            ELSE 'AUTO_SKIP'
        END AS review_tag
    FROM agg_final
),

-- 4. [抽样配置] 计算各组需要抽取的具体数量
-- 关键点：使用 GREATEST(1, ...) 确保只要需要抽样，至少抽一条，避免空样本。
sampling_config AS (
    SELECT 
        stat_date,
        review_tag,
        COUNT(*) as total_cnt,
        CASE
            WHEN review_tag LIKE 'MUST_CHECK_%' THEN COUNT(*)
            WHEN review_tag LIKE 'AUTO_%' THEN 0
            WHEN review_tag = 'SAMPLE_5PCT_STORE' THEN GREATEST(1, CEIL(COUNT(*) * 0.05))
            WHEN review_tag = 'SAMPLE_10PCT_GENERIC' THEN GREATEST(1, CEIL(COUNT(*) * 0.10))
            ELSE 0
        END AS need_pick_count
    FROM rule_final
    GROUP BY stat_date, review_tag
),

-- 5. [随机池] 打乱数据并编号
-- 思考：因果律在这里体现为 deterministic 的随机。ORDER BY random() 是打破原有顺序的钥匙。
randomized_pool AS (
    SELECT 
        r.stat_date,
        r.order_id,
        r.order_no,
        r.start_time,
        r.final_scene_type,
        r.final_score,
        r.review_tag,
        s.total_cnt,
        s.need_pick_count,
        ROW_NUMBER() OVER (
            PARTITION BY r.stat_date, r.review_tag
            ORDER BY random() -- [注意] 部分数仓可能需要使用 rand() 或 specific_random_func()
        ) as rn
    FROM rule_final r
    JOIN sampling_config s
      ON r.stat_date = s.stat_date AND r.review_tag = s.review_tag
)

-- 6. [最终输出] 截取所需数量的数据
SELECT 
    stat_date,
    order_id,
    order_no,
    start_time,
    final_scene_type AS scene_type,
    final_score AS score,
    review_tag,
    total_cnt,
    need_pick_count
FROM randomized_pool
WHERE rn <= need_pick_count
ORDER BY stat_date DESC, review_tag, order_id;
```

## 三、重构的启示

写到一半时，我曾发现原本以为优雅的设计其实耦合严重；原本以为清晰的接口其实语义模糊。比如在 `agg_final` 阶段，我最初试图在 `GROUP BY` 中加入复杂的场景判断逻辑，导致代码冗长且难以维护。

直到我重新审视业务约束——“**同一个订单下场景唯一**”。这个简单的真理让我移除了所有冗余的优先级判断，直接用 `MAX(scene)` 代表全局。这种发现让人沮丧（因为之前的努力似乎白费了），却也让人兴奋——因为这意味着我正在逼近系统的真相，而不仅仅是让它“跑通”。

### 为什么不要等待“完美的架构”？

我们常常陷入一种误区：要等到想清楚所有边缘情况、设计出完美模型才动工。但现实是，**编码本身就是寻找秩序的过程**。

1.  **确定性带来的补偿**：在屏幕前，我们构建一个个微型的、确定的世界。输入 A 必然导致输出 B，`rn <= need_pick_count` 就能精准控制抽样量。这种确定性是对混乱现实的一种补偿。
2.  **拥抱现实的噪声**：然而，真正的山不在屏幕里。自然界没有 `try-catch`，风不会按算法吹拂。我们在代码中建立的秩序，是为了更好地应对现实中的无序。

## 四、结语

这段 SQL 不仅仅是一个工具，它是一次思维的具象化。

*   **对于机器**：它是严谨的指令，内存不会撒谎，逻辑必须闭环。
*   **对于人**：它是与过去自己的对话。也许几年后回看，我会惊讶于当时为何执着于 `GREATEST(1, ...)` 这样的细节，却忽略了更宏观的架构；但也可能欣慰于，正是这些细节保证了系统的健壮。

不要等到有「完美的架构」才动工，也不要等到有「完美的生活」才去体验。

**生活本身就是不断调试**（Debug）

---
*写于SQL执行通过后的下午，窗外晴空，室内阴冷。*
