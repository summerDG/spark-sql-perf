/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpcds

import com.databricks.spark.sql.perf.{Benchmark, ExecutionMode}

trait MultiJoinQueries extends Benchmark {

  import ExecutionMode._

   val queries= {

     Seq(
       ("q7-twoMapJoins",
         """
                          |select
                          |  store_sales.ss_cdemo_sk,
                          |  customer_demographics.cd_demo_sk,
                          |  store_sales.ss_item_sk,
                          |  item.i_item_sk
                          |from
                          |  store_sales
                          |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
                          |  join item on (store_sales.ss_item_sk = item.i_item_sk)
                          |where
                          |  cd_gender = 'F'
                          |  and cd_marital_status = 'W'
                          |  and cd_education_status = 'Primary'
                          |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
                        """
           .stripMargin),

     ("q7-fourMapJoins",
       """
                           |select
                           |  store_sales.ss_cdemo_sk,
                           |  customer_demographics.cd_demo_sk,
                           |  store_sales.ss_item_sk,
                           |  item.i_item_sk,
                           |  store_sales.ss_promo_sk,
                           |  promotion.p_promo_sk,
                           |  ss_sold_date_sk,
                           |  d_date_sk
                           |from
                           |  store_sales
                           |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
                           |  join item on (store_sales.ss_item_sk = item.i_item_sk)
                           |  join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
                           |  join date_dim on (ss_sold_date_sk = d_date_sk)
                           |where
                           |  cd_gender = 'F'
                           |  and cd_marital_status = 'W'
                           |  and cd_education_status = 'Primary'
                           |  and (p_channel_email = 'N'
                           |    or p_channel_event = 'N')
                           |  and d_year = 1998
                           |  -- and ss_date between '1998-01-01' and '1998-12-31'
                           |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
                         """.stripMargin),
     ("q19-fiveMapJoins",
       """
               |select
               |  store_sales.ss_item_sk,
               |  item.i_item_sk,
               |  store_sales.ss_customer_sk,
               |  customer.c_customer_sk,
               |  customer.c_current_addr_sk,
               |  customer_address.ca_address_sk,
               |  store_sales.ss_store_sk,
               |  store.s_store_sk,
               |  store_sales.ss_sold_date_sk,
               |  date_dim.d_date_sk
               |from
               |  store_sales
               |  join item on (store_sales.ss_item_sk = item.i_item_sk)
               |  join customer on (store_sales.ss_customer_sk = customer.c_customer_sk)
               |  join customer_address on (customer.c_current_addr_sk = customer_address.ca_address_sk)
               |  join store on (store_sales.ss_store_sk = store.s_store_sk)
               |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
               |where
               |  i_manager_id=7
               |  and d_moy=11
               |  and d_year=1999
               |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
             """.
         stripMargin),
     ("q82-trianglesMapJoins",
       """
               |select
               |  store_sales.ss_item_sk,
               |  item.i_item_sk,
               |  item.i_item_sk,
               |  inventory.inv_item_sk
               |from
               |  store_sales
               |  join item on (store_sales.ss_item_sk = item.i_item_sk)
               |  join inventory on (item.i_item_sk = inventory.inv_item_sk)
               |  -- join date_dim on (inventory.inv_date_sk = date_dim.d_date_sk)
               |where
               |  i_current_price between 30 and 30 + 30
               |  and i_manufact_id in (437, 129, 727, 663)
               |  and inv_quantity_on_hand between 100 and 500
             """.stripMargin),
     (
       "q3-twoMapJoins", """
      select  dt.d_date_sk,store_sales.ss_sold_date_sk,store_sales.ss_item_sk,item.i_item_sk
        from  date_dim dt
        JOIN store_sales on dt.d_date_sk = store_sales.ss_sold_date_sk
        JOIN item on store_sales.ss_item_sk = item.i_item_sk
        where
        item.i_manufact_id = 436
        and dt.d_moy=12
        and (ss_sold_date_sk between 2451149 and 2451179
        or ss_sold_date_sk between 2451514 and 2451544
        or ss_sold_date_sk between 2451880 and 2451910
        or ss_sold_date_sk between 2452245 and 2452275
        or ss_sold_date_sk between 2452610 and 2452640)
        """)
   ).map { case (name, sqlText) =>
     Query(name = name, sqlText = sqlText, description = "", executionMode = ForeachResults)
   }
   }
//  val queriesMap = queries.map(q => q.name -> q).toMap
//  val interactiveQueries =
//    Seq("q7-twoMapJoins", "q42", "q52", "q55", "q63", "q68", "q73", "q98").map(queriesMap)
}
