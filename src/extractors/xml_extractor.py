"""
XML Extractor — Parses XBRL-style and hierarchical XML files.
Handles XML namespaces, nested hierarchies, and period attributes.
"""

import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType

from src.extractors.base_extractor import BaseExtractor


class XMLExtractor(BaseExtractor):
    """
    Extracts data from XML files.
    
    Supports two XML formats:
    1. XBRL-style (samplexml.xml) — entities with period attributes (CY/PY)
    2. Hierarchical (samplexml2.xml) — Company > Details > Financials
    """

    def extract(self) -> DataFrame:
        """Route to appropriate parser based on source name."""
        if self.logger:
            self.logger.info(f"Extracting XML: {self.source_name} from {self.source_path}")

        if self.source_name == "xbrl_xml":
            df = self._extract_xbrl()
        elif self.source_name == "hierarchical_xml":
            df = self._extract_hierarchical()
        else:
            raise ValueError(f"Unknown XML source: {self.source_name}")

        self._log_extraction(df)
        return df

    def _extract_xbrl(self) -> DataFrame:
        """
        Parse XBRL-style XML with namespaces and period attributes.
        
        Structure:
            <xbrl xmlns="http://www.xbrl.org">
                <entity>
                    <identifier>C001</identifier>
                    <financials period="CY">
                        <revenue>2854</revenue>
                        <profit>878</profit>
                    </financials>
                    <financials period="PY">...</financials>
                </entity>
            </xbrl>
        """
        tree = ET.parse(self.source_path)
        root = tree.getroot()

        # Handle namespace
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        rows = []
        for entity in root.findall(f"{ns}entity"):
            identifier = entity.find(f"{ns}identifier")
            company_id = identifier.text.strip() if identifier is not None else None

            for financials in entity.findall(f"{ns}financials"):
                period = financials.get("period", "UNKNOWN")
                revenue_el = financials.find(f"{ns}revenue")
                profit_el = financials.find(f"{ns}profit")

                rows.append(Row(
                    company_id=company_id,
                    period=period,
                    revenue=revenue_el.text.strip() if revenue_el is not None else None,
                    profit=profit_el.text.strip() if profit_el is not None else None,
                    source_file="samplexml.xml"
                ))

        if self.logger:
            self.logger.info(
                f"XBRL parser: {len(rows)} records from "
                f"{len(rows) // 2 if rows else 0} entities (CY + PY)"
            )

        schema = StructType([
            StructField("company_id", StringType(), True),
            StructField("period", StringType(), True),
            StructField("revenue", StringType(), True),
            StructField("profit", StringType(), True),
            StructField("source_file", StringType(), True),
        ])

        return self.spark.createDataFrame(rows, schema)

    def _extract_hierarchical(self) -> DataFrame:
        """
        Parse hierarchical XML with nested Company > Details > Financials.
        
        Structure:
            <Companies>
                <Company>
                    <CompanyID>C001</CompanyID>
                    <Details>
                        <Name>Company_1</Name>
                        <Financials>
                            <Revenue>2593</Revenue>
                            <Year>2023</Year>
                        </Financials>
                    </Details>
                </Company>
            </Companies>
        """
        tree = ET.parse(self.source_path)
        root = tree.getroot()

        rows = []
        for company in root.findall("Company"):
            company_id_el = company.find("CompanyID")
            company_id = company_id_el.text.strip() if company_id_el is not None else None

            details = company.find("Details")
            if details is not None:
                name_el = details.find("Name")
                company_name = name_el.text.strip() if name_el is not None else None

                financials = details.find("Financials")
                if financials is not None:
                    revenue_el = financials.find("Revenue")
                    year_el = financials.find("Year")

                    rows.append(Row(
                        company_id=company_id,
                        company_name=company_name,
                        revenue=revenue_el.text.strip() if revenue_el is not None else None,
                        year=year_el.text.strip() if year_el is not None else None,
                        source_file="samplexml2.xml"
                    ))

        if self.logger:
            self.logger.info(f"Hierarchical XML parser: {len(rows)} companies extracted")

        schema = StructType([
            StructField("company_id", StringType(), True),
            StructField("company_name", StringType(), True),
            StructField("revenue", StringType(), True),
            StructField("year", StringType(), True),
            StructField("source_file", StringType(), True),
        ])

        return self.spark.createDataFrame(rows, schema)
