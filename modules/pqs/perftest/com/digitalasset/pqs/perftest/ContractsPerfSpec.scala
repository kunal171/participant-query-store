// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.perftest

import com.digitalasset.pqs.services.postgres.Database
import zio.jdbc.*
import zio.ZIO
import com.digitalasset.pqs.services.postgres.Postgres

object ContractsPerfSpec extends SharedHydratedLedgerTest with QueryPerfSpec[Int]:

  def spec = suite("ContractsPerfSpec")(
    perfTest("active") {
      for
        lastIx    <- Database.getLastIx // 29767
        offset100 <- Database.getOffset(lastIx)
        offset50  <- Database.getOffset((lastIx * 0.5).toLong)
        offset10  <- Database.getOffset((lastIx * 0.1).toLong)
        blobA = "Blob:BlobA"
      yield Benchmark(
        // The dedup lateral join probes the contract_id index once per active contract (~2 µs each),
        // serially above the Gather. Its high plan cost activates JIT, a fixed cost that dominates at small offsets.
        Query(
          "select_star_from_all_at_100",
          sql"select * from active(null, $offset100)",
          repeat = 2,
          expectedDiff = 0.5,
          tolerance = 0.4
        ),
        Query(
          "select_star_from_all_at_50",
          sql"select * from active(null, $offset50)",
          repeat = 2,
          expectedDiff = 0.6
        ),
        Query(
          "select_star_from_all_at_10",
          sql"select * from active(null, $offset10)",
          repeat = 8,
          expectedDiff = 1.09,
          tolerance = 0.4
        ),

        // Same serial per-contract probes, no JIT.
        Query(
          "select_star_from_bloba_at_100",
          sql"select * from active($blobA, $offset100)",
          repeat = 4,
          expectedDiff = 0.39
        ),
        Query(
          "select_star_from_bloba_at_50",
          sql"select * from active($blobA, $offset50)",
          repeat = 4,
          expectedDiff = 0.42
        ),
        Query(
          "select_star_from_bloba_at_10",
          sql"select * from active($blobA, $offset10)",
          repeat = 16,
          expectedDiff = 0.43
        ),

        // Baseline is a bare scan with no joins, so the per-contract probes weigh more
        Query(
          "select_cid_from_all_at_100",
          sql"select contract_id from active(null, $offset100)",
          repeat = 2,
          expectedDiff = 0.81
        ),
        Query(
          "select_cid_from_bloba_at_100",
          sql"select contract_id from active($blobA, $offset100)",
          repeat = 4,
          expectedDiff = 0.98
        ),

        // The payload filter runs in the contract scan, so only the matching contracts are probed
        Query(
          "select_star_from_all_at_100_filter_on_payload",
          sql"select * from active(null, $offset100) where payload ->> 'value' = '42'",
          repeat = 4,
          expectedDiff = 0
        ),
        Query(
          "select_star_from_bloba_at_100_filter_on_payload",
          sql"select * from active($blobA, $offset100) where payload ->> 'value' = '42'",
          repeat = 8,
          expectedDiff = -0.03
        ),

        // Only 20 contracts are probed; the diff is the candidate's parallel Gather startup
        Query(
          "select_star_from_all_at_100_limit_20",
          sql"select * from active(null, $offset100) LIMIT 20",
          repeat = 4,
          expectedDiff = 1.00
        ),
        Query(
          "select_star_from_bloba_at_100_limit_20",
          sql"select * from active($blobA, $offset100) where payload ->> 'value' = '42'",
          repeat = 8,
          expectedDiff = -0.02
        )
      )
    }
  )

  override protected def executeQuery(sql: SqlFragment): ZIO[Database, Throwable, Int] =
    Postgres.query(sql).map(_.rows.size)
