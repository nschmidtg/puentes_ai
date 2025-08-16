WITH filtered_needs AS (
    SELECT *
    FROM needs
    WHERE id NOT IN (5191,5199,1220,1579,2139,1767,1249,5207,2022,605,1709,1430,1223,4692,5125,2701,321,5180,784,681,670,5138,1260,637,388,4854,2744,5345,5286,540,594,1216,1431,1261,601,406,2397,543,5176,2706,1224,2915,5287,1435,5392,148,28,3319,1428,1576,5211,1362,689,569,1814,546,1575,287,1593,669,5281,1588,5233,1759,4835,2019,2479,554,320,1633,1082,5195,4910,4912,307,5399,5128,567,2819,579,2483,5206,596,5127,1439,1446,5190,534,5357,1530,1581,34,5210,2461,3055,3513,5263,785,3526,4988,2123,1081,5202,3052,1609,542,5124,1358,3302,4987,5393,1043,27,3472,308,600,5324,2482,1587,5131,5283,1531,351,1427,1607,1211,645,3365,1725,4834,4911,692,1353,1529,2007,1402,548,2820,503,1366,5397,4928,1218,5177,5356,5246,5129,4903,3313,5396,2017,2476,1113,1217,1058,1763,1178,507,1128,1228,2622,1616,1214,3514,544,245,5389,2848,2023,2872,4816,491,2957,2248,1765,1598,1255,1109,2150,5320,715,5327,3127,2912,442,5130,5311,2329,897,5288,1256,5208,31,690,677,5342,369,1361,581,1363,582,848,401,2246,547,1586,5380,5227,1760,1738,870,555,608,5126,5089,4531,251,4131,518,841,2006,5347,323,1123,1841,1030,1234,5088,506,511,2825,3229,2236,3435,2703,2283,241,3228,1722,3207,1028,513,483,717,5209,997,5175,5228,2018,5289,566,3527,5315,5122,1837,1724,4876,2702,1111,4986,2481,1252,861,3524,4804,3224,2016,350,2818,1600,1433,545,26,1481,758,5395,845,5343,1610,609,5260,1250,3314,3318,5313,4030,2029,1761,2959,5205,2231,2021,1025,3470,2024,5279,3471,1780,1436,1528,1227,1782,680,33,2020,671,541,1710,1359,1443,5196,2237,1266,124,1785,1112,5319,783,1726,5398,1351,2110,3515,1762,5394,1254,5179,2846,1783,840,3312,656,5280,5277,1360,1681,504,2958,516,556,1781,1509,3741,5203,5262,305,519,5358,3536,4985,1354,1231,627,1532,1434,3320,1213,1591,2235,1811,3292,1572,5261,5212,346,1982,678)
      AND (
          (internship_description IS NOT NULL AND internship_description != '')
          OR (brief_description IS NOT NULL AND brief_description != '')
      )
),
filtered_resources AS (
    SELECT *
    FROM academic_resources
    WHERE id NOT IN (570,435,386,300,309,437,616,431,320,283,525,307,487,301,620,639,201,606,384,923,648,791,1121,1517)
),
positive_matches AS (
    SELECT DISTINCT
        n.id AS need_id,
        n.name AS need_name,
        regexp_replace(COALESCE(NULLIF(n.internship_description, ''), NULLIF(n.brief_description, '')) , E'[\\n\\r]+', ' ', 'g' ) AS need_description,
        n.expiration_date AS need_expiration_date,
        n.created_at AS need_created_at,
        n.internship AS need_internship,
        o.name AS offer_name,
        regexp_replace(COALESCE(NULLIF(ar.description, ''), NULLIF(o.description, '')) , E'[\\n\\r]+', ' ', 'g' ) AS offer_description,
        o.semester AS offer_semester,
        o.company_year AS offer_company_year,
        o.expiration_date AS offer_expiration_date,
        o.academic_resource_id AS offer_academic_resource_id,
        o.created_at AS offer_created_at,
        ar.id AS academic_resource_id,
        ar.name AS academic_resource_name,
        c.name as cluster_name,
        regexp_replace(ar.description , E'[\\n\\r]+', ' ', 'g' ) AS academic_resource_description,
        ar.level AS academic_resource_level,
        ar.academic_resource_type_id,
        1 AS has_match
    FROM matches m
    JOIN filtered_needs n ON n.id = m.need_id
    JOIN offers o ON o.id = m.offer_id
    JOIN filtered_resources ar ON ar.id = o.academic_resource_id
    join clusters c on c.academic_resource_id = ar.id
    WHERE COALESCE(NULLIF(ar.description, ''), NULLIF(o.description, '')) IS NOT NULL
    AND COALESCE(NULLIF(n.internship_description, ''), NULLIF(n.brief_description, '')) IS NOT NULL
),
negative_matches AS (
    SELECT DISTINCT
        n.id AS need_id,
        n.name AS need_name,
        regexp_replace(COALESCE(NULLIF(n.internship_description, ''), NULLIF(n.brief_description, '')) , E'[\\n\\r]+', ' ', 'g' ) AS need_description,
        n.expiration_date AS need_expiration_date,
        n.created_at AS need_created_at,
        n.internship AS need_internship,
        o.name AS offer_name,
        regexp_replace(COALESCE(NULLIF(ar.description, ''), NULLIF(o.description, '')) , E'[\\n\\r]+', ' ', 'g' ) AS offer_description,
        o.semester AS offer_semester,
        o.company_year AS offer_company_year,
        o.expiration_date AS offer_expiration_date,
        o.academic_resource_id AS offer_academic_resource_id,
        o.created_at AS offer_created_at,
        ar.id AS academic_resource_id,
        ar.name AS academic_resource_name,
        c.name as cluster_name,
        regexp_replace(ar.description , E'[\\n\\r]+', ' ', 'g' ) AS academic_resource_description,
        ar.level AS academic_resource_level,
        ar.academic_resource_type_id,
        0 AS has_match
    FROM (
        SELECT DISTINCT n.*
        FROM matches m
        JOIN filtered_needs n ON n.id = m.need_id
    ) n
    CROSS JOIN offers o
    JOIN filtered_resources ar ON ar.id = o.academic_resource_id
    LEFT JOIN matches m ON m.need_id = n.id AND m.offer_id = o.id
    join clusters c on c.academic_resource_id = ar.id
    WHERE m.id IS NULL
      AND o.created_at BETWEEN n.created_at - INTERVAL '1 months'
                        AND n.created_at + INTERVAL '1 months'
      AND COALESCE(NULLIF(ar.description, ''), NULLIF(o.description, '')) IS NOT NULL
      AND COALESCE(NULLIF(n.internship_description, ''), NULLIF(n.brief_description, '')) IS NOT NULL
)
SELECT * FROM positive_matches
UNION ALL
SELECT * FROM negative_matches;