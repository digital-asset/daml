package com.daml.error.generator

import com.daml.error.{Explanation, Resolution}

case class DocItem(
    className: String,
    category: String,
    hierarchicalGrouping: List[String],
    conveyance: String,
    code: String,
    explanation: Explanation,
    resolution: Resolution,
)
