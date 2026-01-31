#!/usr/bin/env python3
"""
Comprehensive Tableau to Power BI PBIP Converter

This module converts Tableau .twbx files to Power BI Desktop Project (.pbip) format,
including data extraction, formula translation, and visual generation.
"""

import zipfile
import os
import shutil
import json
import uuid
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import csv


class TableauToDAXConverter:
    """Converts Tableau calculated field formulas to DAX measures."""
    
    # Mapping of Tableau functions to DAX equivalents
    FUNCTION_MAPPINGS = {
        # Aggregations
        'SUM': 'SUM',
        'AVG': 'AVERAGE',
        'COUNT': 'COUNT',
        'COUNTD': 'DISTINCTCOUNT',
        'MIN': 'MIN',
        'MAX': 'MAX',
        'MEDIAN': 'MEDIAN',
        'STDEV': 'STDEV.S',
        'VAR': 'VAR.S',
        'ATTR': '',  # No direct equivalent, handle separately
        
        # Date functions
        'YEAR': 'YEAR',
        'MONTH': 'MONTH',
        'DAY': 'DAY',
        'DATEPART': 'DATEPART',
        'DATEADD': 'DATEADD',
        'DATEDIFF': 'DATEDIFF',
        'DATETRUNC': '',  # Handle with STARTOFMONTH, STARTOFYEAR etc.
        'TODAY': 'TODAY',
        'NOW': 'NOW',
        'DATENAME': 'FORMAT',
        
        # String functions
        'LEFT': 'LEFT',
        'RIGHT': 'RIGHT',
        'MID': 'MID',
        'LEN': 'LEN',
        'UPPER': 'UPPER',
        'LOWER': 'LOWER',
        'TRIM': 'TRIM',
        'CONTAINS': 'CONTAINSSTRING',
        'FIND': 'SEARCH',
        'REPLACE': 'SUBSTITUTE',
        'SPLIT': '',  # No direct equivalent
        
        # Logical functions
        'IF': 'IF',
        'CASE': 'SWITCH',
        'WHEN': '',  # Part of CASE/SWITCH
        'THEN': '',
        'ELSE': '',
        'END': '',
        'ELSEIF': '',  # Handle as nested IF
        'IIF': 'IF',
        'IFNULL': 'IF(ISBLANK',
        'ZN': 'IF(ISBLANK',
        'ISNULL': 'ISBLANK',
        'AND': '&&',
        'OR': '||',
        'NOT': 'NOT',
        
        # Math functions
        'ABS': 'ABS',
        'CEILING': 'CEILING',
        'FLOOR': 'FLOOR',
        'ROUND': 'ROUND',
        'POWER': 'POWER',
        'SQRT': 'SQRT',
        'EXP': 'EXP',
        'LN': 'LN',
        'LOG': 'LOG',
        'SIGN': 'SIGN',
        
        # Type conversion
        'INT': 'INT',
        'FLOAT': 'VALUE',
        'STR': 'FORMAT',
        'DATE': 'DATE',
        'DATETIME': 'DATETIME',
        
        # Table calculations (require special handling)
        'INDEX': 'RANKX',
        'FIRST': '',
        'LAST': '',
        'LOOKUP': '',
        'RUNNING_SUM': 'CALCULATE(SUM(...), FILTER(ALLSELECTED(...), ...))',
        'RUNNING_AVG': '',
        'WINDOW_SUM': '',
        'WINDOW_AVG': '',
        'RANK': 'RANKX',
        'RANK_DENSE': 'RANKX',
        'RANK_MODIFIED': 'RANKX',
        'RANK_PERCENTILE': 'PERCENTILE.INC',
        
        # LOD expressions (Level of Detail) - require CALCULATE in DAX
        'FIXED': 'CALCULATE',
        'INCLUDE': 'CALCULATE',
        'EXCLUDE': 'CALCULATE',
    }
    
    # Date part mappings for DATEDIFF and DATEPART
    DATE_PARTS = {
        "'year'": 'YEAR',
        "'quarter'": 'QUARTER', 
        "'month'": 'MONTH',
        "'week'": 'WEEK',
        "'day'": 'DAY',
        "'hour'": 'HOUR',
        "'minute'": 'MINUTE',
        "'second'": 'SECOND',
    }
    
    def __init__(self, table_name: str = "Data", all_tables: List[str] = None):
        self.table_name = table_name
        self.all_tables = all_tables or []
        self.column_mappings = {}  # Maps Tableau field references to DAX column refs
        
    def set_column_mappings(self, columns: Dict[str, str]):
        """Set up column name mappings from Tableau to Power BI."""
        self.column_mappings = columns
        
    def convert_formula(self, tableau_formula: str, is_calculation: bool = True) -> str:
        """
        Convert a Tableau formula to DAX.
        
        Args:
            tableau_formula: The Tableau formula string
            is_calculation: If True, wrap result as a measure
            
        Returns:
            DAX equivalent formula
        """
        if not tableau_formula:
            return ""
            
        # Clean up the formula
        formula = tableau_formula.strip()
        formula = formula.replace('\r\n', '\n').replace('\r', '\n')
        
        # Handle simple literal values
        if self._is_literal(formula):
            return formula
            
        # Handle LOD expressions (Level of Detail)
        formula = self._convert_lod_expressions(formula)
        
        # Convert field references [FieldName] to 'Table'[FieldName]
        formula = self._convert_field_references(formula)
        
        # Convert function calls
        formula = self._convert_functions(formula)
        
        # Convert operators
        formula = self._convert_operators(formula)
        
        # Convert IF/ELSEIF/ELSE/END structures
        formula = self._convert_if_statements(formula)
        
        # Convert CASE statements
        formula = self._convert_case_statements(formula)
        
        # Clean up any remaining issues
        formula = self._cleanup_formula(formula)
        
        return formula
    
    def _is_literal(self, formula: str) -> bool:
        """Check if formula is a simple literal value."""
        # Check for numeric literals
        try:
            float(formula)
            return True
        except ValueError:
            pass
        # Check for string literals
        if formula.startswith('"') and formula.endswith('"'):
            return True
        return False
    
    def _convert_field_references(self, formula: str) -> str:
        """Convert Tableau field references to DAX column references."""
        # Pattern to match [field_name] but not already converted 'Table'[field_name]
        # Also handle datasource-qualified references like [datasource].[field]
        
        # First handle qualified references like [Parameters].[Base Salary]
        pattern_qualified = r'\[([^\]]+)\]\.\[([^\]]+)\]'
        
        def replace_qualified(match):
            table = match.group(1)
            field = match.group(2)
            # Clean up table and field names
            table = self._clean_name(table)
            field = self._clean_name(field)
            return f"'{table}'[{field}]"
            
        formula = re.sub(pattern_qualified, replace_qualified, formula)
        
        # Then handle simple references like [Sales]
        pattern_simple = r'(?<!\')(?<!\w)\[([^\]]+)\]'
        
        def replace_simple(match):
            field = match.group(1)
            # Skip if it's a string literal or already part of a table reference
            if field.startswith('"') or field.startswith("'"):
                return match.group(0)
            field = self._clean_name(field)
            return f"'{self.table_name}'[{field}]"
            
        formula = re.sub(pattern_simple, replace_simple, formula)
        
        return formula
    
    def _clean_name(self, name: str) -> str:
        """Clean up a field/table name for DAX."""
        # Remove special prefixes that Tableau uses
        prefixes_to_remove = ['none:', 'sum:', 'avg:', 'min:', 'max:', 'cnt:', 
                              'cntd:', 'attr:', 'usr:', 'mn:', 'yr:', 'qr:']
        for prefix in prefixes_to_remove:
            if name.lower().startswith(prefix):
                name = name[len(prefix):]
        # Remove suffixes like :nk, :ok, :qk
        suffixes = [':nk', ':ok', ':qk']
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        return name.strip()
    
    def _convert_lod_expressions(self, formula: str) -> str:
        """Convert Tableau LOD expressions to DAX CALCULATE."""
        # Pattern for {FIXED [dim1], [dim2] : AGG([measure])}
        lod_pattern = r'\{(fixed|include|exclude)\s+([^:]+):\s*([^}]+)\}'
        
        def convert_lod(match):
            lod_type = match.group(1).upper()
            dimensions = match.group(2).strip()
            expression = match.group(3).strip()
            
            # Parse dimensions
            dims = [d.strip() for d in dimensions.split(',')]
            dim_refs = [self._convert_field_references(f"[{d.strip('[]')}]") for d in dims]
            
            if lod_type == 'FIXED':
                # FIXED = Calculate aggregation at specific dimension level
                all_values = ', '.join([f'ALL({d})' for d in dim_refs])
                return f'CALCULATE({expression}, ALLEXCEPT({self.table_name}, {", ".join(dim_refs)}))'
            elif lod_type == 'INCLUDE':
                return f'CALCULATE({expression})'
            else:  # EXCLUDE
                return f'CALCULATE({expression}, REMOVEFILTERS({", ".join(dim_refs)}))'
                
        formula = re.sub(lod_pattern, convert_lod, formula, flags=re.IGNORECASE)
        return formula
    
    def _convert_functions(self, formula: str) -> str:
        """Convert Tableau function calls to DAX equivalents."""
        for tableau_func, dax_func in self.FUNCTION_MAPPINGS.items():
            if dax_func:  # Only process if there's a direct mapping
                # Case-insensitive function replacement
                pattern = rf'\b{tableau_func}\s*\('
                formula = re.sub(pattern, f'{dax_func}(', formula, flags=re.IGNORECASE)
        
        # Special handling for DATEDIFF
        formula = self._convert_datediff(formula)
        
        # Special handling for ZN (null to zero)
        formula = self._convert_zn(formula)
        
        # Special handling for IFNULL
        formula = self._convert_ifnull(formula)
        
        return formula
    
    def _convert_datediff(self, formula: str) -> str:
        """Convert Tableau DATEDIFF to DAX DATEDIFF."""
        # Tableau: DATEDIFF('day', [Start Date], [End Date])
        # DAX: DATEDIFF([Start Date], [End Date], DAY)
        pattern = r"DATEDIFF\s*\(\s*'(\w+)'\s*,\s*([^,]+)\s*,\s*([^)]+)\)"
        
        def replace_datediff(match):
            date_part = match.group(1).upper()
            start_date = match.group(2).strip()
            end_date = match.group(3).strip()
            return f'DATEDIFF({start_date}, {end_date}, {date_part})'
            
        return re.sub(pattern, replace_datediff, formula, flags=re.IGNORECASE)
    
    def _convert_zn(self, formula: str) -> str:
        """Convert ZN (Zero if Null) to DAX."""
        # Tableau: ZN([Field])
        # DAX: IF(ISBLANK([Field]), 0, [Field])
        pattern = r'ZN\s*\(\s*([^)]+)\s*\)'
        
        def replace_zn(match):
            field = match.group(1).strip()
            return f'IF(ISBLANK({field}), 0, {field})'
            
        return re.sub(pattern, replace_zn, formula, flags=re.IGNORECASE)
    
    def _convert_ifnull(self, formula: str) -> str:
        """Convert IFNULL to DAX COALESCE."""
        # Tableau: IFNULL([Field], replacement)
        # DAX: COALESCE([Field], replacement)
        pattern = r'IFNULL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)'
        
        def replace_ifnull(match):
            field = match.group(1).strip()
            replacement = match.group(2).strip()
            return f'COALESCE({field}, {replacement})'
            
        return re.sub(pattern, replace_ifnull, formula, flags=re.IGNORECASE)
    
    def _convert_operators(self, formula: str) -> str:
        """Convert Tableau operators to DAX equivalents."""
        # String concatenation: + in Tableau -> & in DAX for strings
        # This is tricky as + is also arithmetic addition
        # For now, leave as is - DAX handles both
        
        # Comparison operators are the same
        # Logical: AND -> &&, OR -> ||
        formula = re.sub(r'\bAND\b', '&&', formula, flags=re.IGNORECASE)
        formula = re.sub(r'\bOR\b', '||', formula, flags=re.IGNORECASE)
        
        # Not equal: <> is same in both
        # Equal: = is same in both
        
        return formula
    
    def _convert_if_statements(self, formula: str) -> str:
        """Convert Tableau IF/ELSEIF/ELSE/END to DAX IF structure."""
        # Tableau: IF cond THEN val1 ELSEIF cond2 THEN val2 ELSE val3 END
        # DAX: IF(cond, val1, IF(cond2, val2, val3))
        
        # First, count how many IF/ELSEIF statements we have to determine nesting
        if_count = len(re.findall(r'\bIF\b', formula, flags=re.IGNORECASE))
        elseif_count = len(re.findall(r'\bELSEIF\b', formula, flags=re.IGNORECASE))
        total_ifs = if_count + elseif_count
        
        # Make sure IF is uppercase and has opening paren
        formula = re.sub(r'\bIF\s+', 'IF(', formula, flags=re.IGNORECASE)
        
        # Replace THEN with ,
        formula = re.sub(r'\bTHEN\b', ',', formula, flags=re.IGNORECASE)
        
        # Replace ELSEIF with , IF(
        formula = re.sub(r'\bELSEIF\b', ', IF(', formula, flags=re.IGNORECASE)
        formula = re.sub(r'\belseif\b', ', IF(', formula)
        
        # Replace ELSE with ,
        formula = re.sub(r'\bELSE\b', ',', formula, flags=re.IGNORECASE)
        
        # Replace END with closing parentheses for all nested IFs
        formula = re.sub(r'\bEND\b', ')' * total_ifs, formula, flags=re.IGNORECASE)
        
        return formula
    
    def _convert_case_statements(self, formula: str) -> str:
        """Convert Tableau CASE statements to DAX SWITCH."""
        # Tableau: CASE [Field] WHEN 'value1' THEN result1 WHEN 'value2' THEN result2 ELSE default END
        # DAX: SWITCH([Field], "value1", result1, "value2", result2, default)
        
        # Match CASE ... END blocks
        pattern = r'CASE\s+(.+?)\s+END'
        
        def convert_case(match):
            content = match.group(1)
            
            # Find the field (first token before WHEN)
            field_match = re.match(r'(\[?[^\s\[]+(?:\[.+?\])?\]?)\s+WHEN', content, flags=re.IGNORECASE)
            if not field_match:
                return match.group(0)
            
            field = field_match.group(1)
            when_content = content[field_match.end()-4:]  # Include WHEN
            
            # Parse WHEN clauses
            parts = []
            # Handle both quoted and numeric values
            when_pattern = r'WHEN\s+"([^"]+)"\s+THEN\s+(\d+)'
            for when_match in re.finditer(when_pattern, when_content, flags=re.IGNORECASE):
                value = when_match.group(1)
                result = when_match.group(2).strip()
                parts.append(f'"{value}", {result}')
            
            # Check for ELSE clause
            else_pattern = r'ELSE\s+(\d+)\s*$'
            else_match = re.search(else_pattern, when_content, flags=re.IGNORECASE)
            if else_match:
                parts.append(else_match.group(1).strip())
            
            if parts:
                return f'SWITCH({field}, {", ".join(parts)})'
            return match.group(0)  # Return original if no matches
        
        formula = re.sub(pattern, convert_case, formula, flags=re.IGNORECASE | re.DOTALL)
        return formula
    
    def _cleanup_formula(self, formula: str) -> str:
        """Clean up the formula after conversion."""
        # Remove extra whitespace
        formula = re.sub(r'\s+', ' ', formula)
        
        # Fix double parentheses
        formula = formula.replace('((', '(').replace('))', ')')
        
        # Fix comma spacing
        formula = re.sub(r',\s*,', ',', formula)
        formula = re.sub(r'\(\s*,', '(', formula)
        formula = re.sub(r',\s*\)', ')', formula)
        
        return formula.strip()


class PBIPGenerator:
    """Generates Power BI Desktop Project (PBIP) folders."""
    
    def __init__(self, template_path: str, output_path: str, project_name: str):
        self.template_path = Path(template_path)
        self.output_path = Path(output_path)
        self.project_name = project_name
        self.report_folder = self.output_path / f"{project_name}.Report"
        self.model_folder = self.output_path / f"{project_name}.SemanticModel"
        
    def create_structure(self):
        """Create the PBIP folder structure based on template."""
        if self.output_path.exists():
            shutil.rmtree(self.output_path)
        
        # Copy template structure
        shutil.copytree(self.template_path, self.output_path)
        
        # Rename folders and files to match project name
        self._rename_project_files()
        
    def _rename_project_files(self):
        """Rename template files to match the new project name."""
        # Rename .pbip file
        old_pbip = self.output_path / "Sample.pbip"
        new_pbip = self.output_path / f"{self.project_name}.pbip"
        if old_pbip.exists():
            old_pbip.rename(new_pbip)
        
        # Rename report folder
        old_report = self.output_path / "Sample.Report"
        if old_report.exists():
            old_report.rename(self.report_folder)
        
        # Rename model folder
        old_model = self.output_path / "Sample.SemanticModel"
        if old_model.exists():
            old_model.rename(self.model_folder)
        
        # Update references in files
        self._update_pbip_content(new_pbip)
        self._update_platform_files()
        
    def _update_pbip_content(self, pbip_file: Path):
        """Update the .pbip file content with new project name."""
        content = {
            "version": "1.0",
            "artifacts": [
                {
                    "report": {
                        "path": f"{self.project_name}.Report"
                    }
                }
            ],
            "settings": {
                "enableAutoRecovery": True
            }
        }
        with open(pbip_file, 'w') as f:
            json.dump(content, f, indent=2)
    
    def _update_platform_files(self):
        """Update .platform files with new metadata."""
        # Update report .platform
        report_platform = self.report_folder / ".platform"
        if report_platform.exists():
            content = {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                "metadata": {
                    "type": "Report",
                    "displayName": self.project_name
                },
                "config": {
                    "version": "2.0",
                    "logicalId": str(uuid.uuid4())
                }
            }
            with open(report_platform, 'w') as f:
                json.dump(content, f, indent=2)
        
        # Update model .platform
        model_platform = self.model_folder / ".platform"
        if model_platform.exists():
            content = {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                "metadata": {
                    "type": "SemanticModel",
                    "displayName": self.project_name
                },
                "config": {
                    "version": "2.0",
                    "logicalId": str(uuid.uuid4())
                }
            }
            with open(model_platform, 'w') as f:
                json.dump(content, f, indent=2)
    
    def _update_definition_pbir(self):
        """Update the definition.pbir file with model reference."""
        pbir_file = self.report_folder / "definition.pbir"
        content = {
            "version": "4.0",
            "datasetReference": {
                "byPath": {
                    "path": f"../{self.project_name}.SemanticModel"
                }
            }
        }
        with open(pbir_file, 'w') as f:
            json.dump(content, f, indent=2)
            
    def generate_semantic_model(self, tables: List[Dict], measures: List[Dict], relationships: List[Dict] = None):
        """Generate the semantic model TMDL files."""
        definition_folder = self.model_folder / "definition"
        definition_folder.mkdir(parents=True, exist_ok=True)
        
        # Generate model.tmdl with table references
        self._generate_model_tmdl(definition_folder, tables)
        
        # Generate database.tmdl
        self._generate_database_tmdl(definition_folder)
        
        # Generate table files
        tables_folder = definition_folder / "tables"
        tables_folder.mkdir(exist_ok=True)
        
        for table in tables:
            self._generate_table_tmdl(tables_folder, table, measures)
        
        # Generate relationships file if any
        if relationships:
            self._generate_relationships_tmdl(definition_folder, relationships)
        
        # Generate cultures
        cultures_folder = definition_folder / "cultures"
        cultures_folder.mkdir(exist_ok=True)
        self._generate_culture_tmdl(cultures_folder)
        
        # Update definition.pbism
        self._update_definition_pbism()
        
    def _generate_model_tmdl(self, folder: Path, tables: List[Dict] = None):
        """Generate model.tmdl file."""
        lines = [
            "model Model",
            "\tculture: en-US",
            "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
            "\tsourceQueryCulture: en-US",
            "\tdataAccessOptions",
            "\t\tlegacyRedirects",
            "\t\treturnErrorValuesAsNull",
            "",
            "annotation __PBI_TimeIntelligenceEnabled = 1",
            "",
            "annotation PBI_ProTooling = [\"DevMode\"]",
            "",
            "ref cultureInfo en-US",
            ""
        ]
        
        # Add unique table references
        if tables:
            lines.append("")
            seen_tables = set()
            for table in tables:
                table_name = table['name']
                if table_name not in seen_tables:
                    seen_tables.add(table_name)
                    lines.append(f"ref table '{table_name}'")
        
        lines.append("")
        
        with open(folder / "model.tmdl", 'w') as f:
            f.write('\n'.join(lines))
    
    def _generate_database_tmdl(self, folder: Path):
        """Generate database.tmdl file."""
        content = """database
\tcompatibilityLevel: 1600

"""
        with open(folder / "database.tmdl", 'w') as f:
            f.write(content)
    
    def _generate_table_tmdl(self, folder: Path, table: Dict, all_measures: List[Dict]):
        """Generate a table TMDL file."""
        table_name = table['name']
        columns = table.get('columns', [])
        
        # Filter measures for this table
        table_measures = [m for m in all_measures if m.get('table', table_name) == table_name]
        
        # Start building TMDL content
        lines = [f"table '{table_name}'"]
        lines.append(f"\tlineageTag: {uuid.uuid4()}")
        lines.append("")
        
        # Add columns
        for col in columns:
            col_name = col['name']
            # Skip internal Tableau columns
            if col_name.startswith('__') or ':' in col_name:
                continue
            col_type = self._map_datatype(col.get('datatype', 'string'))
            
            lines.append(f"\tcolumn '{col_name}'")
            lines.append(f"\t\tdataType: {col_type}")
            lines.append(f"\t\tlineageTag: {uuid.uuid4()}")
            if col.get('is_hidden'):
                lines.append("\t\tisHidden")
            lines.append(f"\t\tsummarizeBy: {self._get_summarize_by(col)}")
            lines.append(f"\t\tsourceColumn: {col_name}")
            lines.append("")
            lines.append(f"\t\tannotation SummarizationSetBy = Automatic")
            lines.append("")
        
        # Add measures
        for measure in table_measures:
            measure_name = measure.get('caption') or measure['name']
            expression = measure.get('dax_expression', '0')
            
            # Clean up measure name
            measure_name = self._clean_measure_name(measure_name)
            
            # Replace table references to match our table names
            expression = self._fix_table_references(expression, table_name)
            
            lines.append(f"\tmeasure '{measure_name}' = ")
            # Format expression with proper indentation
            expr_lines = expression.split('\n')
            for i, line in enumerate(expr_lines):
                if i == 0:
                    lines[-1] += line
                else:
                    lines.append(f"\t\t\t{line}")
            lines.append(f"\t\tlineageTag: {uuid.uuid4()}")
            if measure.get('format_string'):
                lines.append(f"\t\tformatString: {measure['format_string']}")
            lines.append("")
        
        # Add partition (data source)
        if table.get('source_type') == 'csv':
            lines.extend(self._generate_csv_partition(table))
        elif table.get('source_type') == 'excel':
            lines.extend(self._generate_excel_partition(table))
        else:
            lines.extend(self._generate_default_partition(table))
        
        # Write file
        safe_name = re.sub(r'[^\w\-]', '_', table_name)
        with open(folder / f"{safe_name}.tmdl", 'w') as f:
            f.write('\n'.join(lines))
    
    def _fix_table_references(self, expression: str, default_table: str) -> str:
        """Fix table references in DAX expressions to use correct table names."""
        # Replace generic 'Orders' or 'Data' references with actual table names
        expression = re.sub(r"'Orders'\[", f"'{default_table}'[", expression)
        expression = re.sub(r"'Data'\[", f"'{default_table}'[", expression)
        
        # Fix federated table references
        expression = re.sub(r"'federated\.\w+'\[", f"'{default_table}'[", expression)
        
        return expression
    
    def _generate_csv_partition(self, table: Dict) -> List[str]:
        """Generate partition for CSV data source."""
        table_name = table['name']
        file_path = table.get('file_path', '')
        
        # Get columns for type conversion
        columns = table.get('columns', [])
        type_conversions = []
        for col in columns:
            col_name = col['name']
            if ':' in col_name or col_name.startswith('__'):
                continue
            m_type = self._m_datatype(col.get('datatype', 'string'))
            type_conversions.append(f'{{\"{col_name}\", {m_type}}}')
        
        lines = [
            f"\tpartition '{table_name}' = m",
            "\t\tmode: import",
            "\t\tsource =",
            "\t\t\tlet",
            f"\t\t\t\tSource = Csv.Document(File.Contents(\"Data/Superstore/{os.path.basename(file_path)}\"), [Delimiter=\",\", Encoding=65001, QuoteStyle=QuoteStyle.None]),",
            "\t\t\t\tPromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true])",
        ]
        
        if type_conversions:
            lines.append(f"\t\t\t\t, ChangedTypes = Table.TransformColumnTypes(PromotedHeaders, {{{', '.join(type_conversions)}}})")
            lines.append("\t\t\tin")
            lines.append("\t\t\t\tChangedTypes")
        else:
            lines.append("\t\t\tin")
            lines.append("\t\t\t\tPromotedHeaders")
        
        lines.append("")
        return lines
    
    def _generate_excel_partition(self, table: Dict) -> List[str]:
        """Generate partition for Excel data source."""
        table_name = table['name']
        file_path = table.get('file_path', '')
        sheet_name = table.get('sheet_name', 'Sheet1')
        
        # Get the actual filename
        filename = os.path.basename(file_path) if file_path else f"{table_name}.xlsx"
        
        # Get columns for type conversion
        columns = table.get('columns', [])
        type_conversions = []
        for col in columns:
            col_name = col['name']
            if ':' in col_name or col_name.startswith('__'):
                continue
            m_type = self._m_datatype(col.get('datatype', 'string'))
            type_conversions.append(f'{{\"{col_name}\", {m_type}}}')
        
        # Use safe variable name
        safe_sheet = re.sub(r'[^\w]', '_', sheet_name)
        
        lines = [
            f"\tpartition '{table_name}' = m",
            "\t\tmode: import",
            "\t\tsource =",
            "\t\t\tlet",
            f"\t\t\t\tSource = Excel.Workbook(File.Contents(\"Data/Superstore/{filename}\"), null, true),",
            f"\t\t\t\t{safe_sheet}_Sheet = Source{{[Item=\"{sheet_name}\",Kind=\"Sheet\"]}}[Data],",
            f"\t\t\t\tPromotedHeaders = Table.PromoteHeaders({safe_sheet}_Sheet, [PromoteAllScalars=true])",
        ]
        
        if type_conversions:
            lines.append(f"\t\t\t\t, ChangedTypes = Table.TransformColumnTypes(PromotedHeaders, {{{', '.join(type_conversions)}}})")
            lines.append("\t\t\tin")
            lines.append("\t\t\t\tChangedTypes")
        else:
            lines.append("\t\t\tin")
            lines.append("\t\t\t\tPromotedHeaders")
        
        lines.append("")
        return lines
    
    def _generate_default_partition(self, table: Dict) -> List[str]:
        """Generate a default partition for static data."""
        table_name = table['name']
        columns = table.get('columns', [])
        
        # Create column definitions for Table.FromRows
        col_defs = ', '.join([f'{{"Name": "{c["name"]}", "Type": {self._m_datatype(c.get("datatype", "string"))}}}' for c in columns])
        
        return [
            f"\tpartition '{table_name}' = m",
            "\t\tmode: import",
            "\t\tsource =",
            "\t\t\tlet",
            f"\t\t\t\tSource = #table(type table [{', '.join([c['name'] + ' = text' for c in columns])}], {{}})",
            "\t\t\tin",
            "\t\t\t\tSource",
            ""
        ]
    
    def _generate_relationships_tmdl(self, folder: Path, relationships: List[Dict]):
        """Generate relationships.tmdl file."""
        lines = []
        for rel in relationships:
            from_table = rel['from_table']
            from_col = rel['from_column']
            to_table = rel['to_table']
            to_col = rel['to_column']
            
            lines.append(f"relationship {uuid.uuid4()}")
            lines.append(f"\tfromColumn: '{from_table}'[{from_col}]")
            lines.append(f"\ttoColumn: '{to_table}'[{to_col}]")
            lines.append(f"\tcrossFilteringBehavior: bothDirections")
            lines.append("")
        
        if lines:
            with open(folder / "relationships.tmdl", 'w') as f:
                f.write('\n'.join(lines))
    
    def _generate_culture_tmdl(self, folder: Path):
        """Generate culture TMDL file."""
        content = """cultureInfo en-US

\tlinguisticMetadata =
\t\t\t{
\t\t\t  "Version": "1.0.0",
\t\t\t  "Language": "en-US"
\t\t\t}
\t\tcontentType: json

"""
        with open(folder / "en-US.tmdl", 'w') as f:
            f.write(content)
    
    def _update_definition_pbism(self):
        """Update definition.pbism file."""
        content = {
            "version": "4.2",
            "settings": {}
        }
        with open(self.model_folder / "definition.pbism", 'w') as f:
            json.dump(content, f, indent=2)
    
    def _map_datatype(self, tableau_type: str) -> str:
        """Map Tableau data type to DAX/TMDL data type."""
        mapping = {
            'string': 'string',
            'integer': 'int64',
            'real': 'double',
            'date': 'dateTime',
            'datetime': 'dateTime',
            'boolean': 'boolean',
            'table': 'string',  # Tableau internal reference
        }
        return mapping.get(tableau_type.lower(), 'string')
    
    def _m_datatype(self, tableau_type: str) -> str:
        """Map Tableau data type to M/Power Query type."""
        mapping = {
            'string': 'type text',
            'integer': 'Int64.Type',
            'real': 'type number',
            'date': 'type date',
            'datetime': 'type datetime',
            'boolean': 'type logical',
        }
        return mapping.get(tableau_type.lower(), 'type text')
    
    def _get_summarize_by(self, column: Dict) -> str:
        """Get the summarize by setting for a column."""
        role = column.get('role', '')
        datatype = column.get('datatype', '')
        
        if role == 'measure' or datatype in ['real', 'integer']:
            return 'sum'
        return 'none'
    
    def _clean_measure_name(self, name: str) -> str:
        """Clean up a measure name for TMDL."""
        # Remove brackets
        name = name.strip('[]')
        # Remove internal Tableau prefixes
        name = re.sub(r'^Calculation_\d+', '', name)
        if name == '':
            return 'Measure'
        return name
    
    def generate_report_pages(self, worksheets: List[Dict], dashboards: List[Dict]):
        """Generate report pages for each dashboard and worksheet."""
        pages_folder = self.report_folder / "definition" / "pages"
        pages_folder.mkdir(parents=True, exist_ok=True)
        
        page_order = []
        
        # Generate pages for dashboards first (these are the main reports)
        for i, dashboard in enumerate(dashboards):
            page_id = self._generate_page_id()
            page_order.append(page_id)
            self._create_page(pages_folder, page_id, dashboard['name'], 
                            dashboard.get('zones', []), i == 0)
        
        # Generate pages for standalone worksheets
        for worksheet in worksheets:
            ws_name = worksheet['name']
            # Skip worksheets that are already part of dashboards
            if any(ws_name in str(d.get('zones', [])) for d in dashboards):
                continue
            page_id = self._generate_page_id()
            page_order.append(page_id)
            self._create_worksheet_page(pages_folder, page_id, worksheet)
        
        # Generate pages.json
        self._generate_pages_json(pages_folder, page_order)
        
    def _generate_page_id(self) -> str:
        """Generate a unique page ID."""
        return uuid.uuid4().hex[:20]
    
    def _create_page(self, pages_folder: Path, page_id: str, name: str, 
                     zones: List[Dict], is_active: bool = False):
        """Create a report page folder and files."""
        page_folder = pages_folder / page_id
        page_folder.mkdir(exist_ok=True)
        
        # Generate page.json
        page_content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
            "name": page_id,
            "displayName": name,
            "displayOption": "FitToPage",
            "height": 720,
            "width": 1280
        }
        
        with open(page_folder / "page.json", 'w') as f:
            json.dump(page_content, f, indent=2)
        
        # Generate visuals for zones
        visuals_folder = page_folder / "visuals"
        visuals_folder.mkdir(exist_ok=True)
        
        self._create_visuals_from_zones(visuals_folder, zones)
    
    def _create_worksheet_page(self, pages_folder: Path, page_id: str, worksheet: Dict):
        """Create a page from a single worksheet."""
        page_folder = pages_folder / page_id
        page_folder.mkdir(exist_ok=True)
        
        ws_name = worksheet['name']
        
        page_content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
            "name": page_id,
            "displayName": ws_name,
            "displayOption": "FitToPage",
            "height": 720,
            "width": 1280
        }
        
        with open(page_folder / "page.json", 'w') as f:
            json.dump(page_content, f, indent=2)
        
        # Create a single visual for the worksheet
        visuals_folder = page_folder / "visuals"
        visuals_folder.mkdir(exist_ok=True)
        
        visual_id = uuid.uuid4().hex[:20]
        visual_folder = visuals_folder / visual_id
        visual_folder.mkdir(exist_ok=True)
        
        visual_type = self._infer_visual_type(worksheet)
        visual_content = self._create_visual_json(visual_id, ws_name, visual_type,
                                                  x=40, y=40, width=1200, height=640)
        
        with open(visual_folder / "visual.json", 'w') as f:
            json.dump(visual_content, f, indent=2)
    
    def _create_visuals_from_zones(self, visuals_folder: Path, zones: List[Dict]):
        """Create visual folders from dashboard zones."""
        for zone in zones:
            zone_name = zone.get('name')
            if not zone_name or zone_name == 'null':
                continue
                
            visual_id = uuid.uuid4().hex[:20]
            visual_folder = visuals_folder / visual_id
            visual_folder.mkdir(exist_ok=True)
            
            # Convert zone coordinates (Tableau uses 100000 scale)
            x = int(float(zone.get('x', 0)) * 1280 / 100000)
            y = int(float(zone.get('y', 0)) * 720 / 100000)
            width = int(float(zone.get('width', 50000)) * 1280 / 100000)
            height = int(float(zone.get('height', 50000)) * 720 / 100000)
            
            # Ensure minimum sizes
            width = max(width, 100)
            height = max(height, 100)
            
            visual_content = self._create_visual_json(visual_id, zone_name, "clusteredColumnChart",
                                                      x=x, y=y, width=width, height=height)
            
            with open(visual_folder / "visual.json", 'w') as f:
                json.dump(visual_content, f, indent=2)
    
    def _infer_visual_type(self, worksheet: Dict) -> str:
        """Infer the appropriate Power BI visual type from worksheet metadata."""
        ws_name = worksheet['name'].lower()
        
        # Infer based on worksheet name
        if 'map' in ws_name or 'geo' in ws_name:
            return "map"
        elif 'scatter' in ws_name:
            return "scatterChart"
        elif 'line' in ws_name or 'trend' in ws_name:
            return "lineChart"
        elif 'pie' in ws_name or 'donut' in ws_name:
            return "pieChart"
        elif 'bar' in ws_name:
            return "clusteredBarChart"
        elif 'table' in ws_name or 'detail' in ws_name or 'sheet' in ws_name:
            return "pivotTable"
        elif 'kpi' in ws_name or 'total' in ws_name or 'summary' in ws_name:
            return "card"
        else:
            return "clusteredColumnChart"
    
    def _create_visual_json(self, visual_id: str, name: str, visual_type: str,
                           x: int = 0, y: int = 0, width: int = 400, height: int = 300) -> Dict:
        """Create a visual.json structure."""
        return {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visual/1.2.0/schema.json",
            "name": visual_id,
            "visual": {
                "visualType": visual_type,
                "objects": {
                    "title": [
                        {
                            "properties": {
                                "text": {
                                    "expr": {
                                        "Literal": {
                                            "Value": f"'{name}'"
                                        }
                                    }
                                },
                                "show": {
                                    "expr": {
                                        "Literal": {
                                            "Value": "true"
                                        }
                                    }
                                }
                            }
                        }
                    ]
                },
                "visualContainerObjects": {
                    "background": [
                        {
                            "properties": {
                                "show": {
                                    "expr": {
                                        "Literal": {
                                            "Value": "true"
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "position": {
                "x": x,
                "y": y,
                "z": 0,
                "width": width,
                "height": height
            }
        }
    
    def _generate_pages_json(self, pages_folder: Path, page_order: List[str]):
        """Generate pages.json file."""
        content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
            "pageOrder": page_order,
            "activePageName": page_order[0] if page_order else ""
        }
        
        with open(pages_folder / "pages.json", 'w') as f:
            json.dump(content, f, indent=2)


class TableauToPBIPConverter:
    """Main converter class that orchestrates the full conversion."""
    
    def __init__(self, twbx_path: str, template_path: str, output_path: str):
        self.twbx_path = Path(twbx_path)
        self.template_path = Path(template_path)
        self.output_path = Path(output_path)
        self.temp_folder = Path(output_path).parent / "temp_extraction"
        
        self.twb_content = None
        self.data_files = {}
        self.metadata = {
            "data_sources": [],
            "calculations": [],
            "worksheets": [],
            "dashboards": [],
            "parameters": []
        }
        
    def convert(self):
        """Run the full conversion process."""
        print("Starting Tableau to PBIP conversion...")
        
        # Step 1: Extract twbx
        print("\n1. Extracting .twbx file...")
        self._extract_twbx()
        
        # Step 2: Parse TWB XML
        print("\n2. Parsing Tableau workbook...")
        self._parse_twb()
        
        # Step 3: Extract data from embedded files
        print("\n3. Processing embedded data files...")
        self._process_data_files()
        
        # Step 4: Convert calculations to DAX
        print("\n4. Converting calculations to DAX...")
        self._convert_calculations()
        
        # Step 5: Generate PBIP structure
        print("\n5. Generating Power BI project structure...")
        self._generate_pbip()
        
        # Step 6: Copy data files
        print("\n6. Copying data files...")
        self._copy_data_files()
        
        # Cleanup
        print("\n7. Cleaning up temporary files...")
        self._cleanup()
        
        print(f"\nConversion complete! PBIP project created at: {self.output_path}")
        
    def _extract_twbx(self):
        """Extract the .twbx file contents."""
        if self.temp_folder.exists():
            shutil.rmtree(self.temp_folder)
        self.temp_folder.mkdir(parents=True)
        
        with zipfile.ZipFile(self.twbx_path, 'r') as zip_ref:
            zip_ref.extractall(self.temp_folder)
            
            # Find the .twb file
            for file in zip_ref.namelist():
                if file.endswith('.twb'):
                    with zip_ref.open(file) as f:
                        self.twb_content = f.read()
                    break
                    
            # Track data files
            for file in zip_ref.namelist():
                if file.startswith('Data/'):
                    self.data_files[file] = self.temp_folder / file
                    
    def _parse_twb(self):
        """Parse the TWB XML content."""
        root = ET.fromstring(self.twb_content)
        
        # Parse datasources
        for ds in root.findall('.//datasource'):
            ds_name = ds.get('name', '')
            ds_caption = ds.get('caption', ds_name)
            
            source_meta = {
                "name": ds_name,
                "caption": ds_caption,
                "columns": [],
                "calculations": [],
                "connection": None
            }
            
            # Get connection info
            connection = ds.find('.//connection[@class="federated"]')
            if connection:
                named_conn = connection.find('.//named-connection')
                if named_conn:
                    inner_conn = named_conn.find('connection')
                    if inner_conn:
                        source_meta["connection"] = {
                            "class": inner_conn.get('class'),
                            "filename": inner_conn.get('filename', ''),
                            "server": inner_conn.get('server', '')
                        }
            
            # Extract columns and calculations
            for col in ds.findall('.//column'):
                col_name = col.get('name', '')
                col_caption = col.get('caption', col_name)
                col_datatype = col.get('datatype', 'string')
                col_role = col.get('role', '')
                
                calc = col.find('calculation')
                if calc is not None:
                    formula = calc.get('formula', '')
                    self.metadata["calculations"].append({
                        "datasource": ds_caption or ds_name,
                        "name": col_name,
                        "caption": col_caption,
                        "formula": formula,
                        "datatype": col_datatype,
                        "role": col_role
                    })
                else:
                    # Regular column
                    source_meta["columns"].append({
                        "name": col_name.strip('[]'),
                        "caption": col_caption,
                        "datatype": col_datatype,
                        "role": col_role
                    })
            
            # Get columns from metadata records
            for meta_col in ds.findall('.//metadata-record[@class="column"]'):
                col_name = meta_col.find('local-name')
                col_type = meta_col.find('local-type')
                remote_name = meta_col.find('remote-name')
                
                if col_name is not None and remote_name is not None:
                    name = remote_name.text if remote_name.text else col_name.text.strip('[]')
                    dtype = col_type.text if col_type is not None else 'string'
                    
                    # Check if already exists
                    if not any(c['name'] == name for c in source_meta["columns"]):
                        source_meta["columns"].append({
                            "name": name,
                            "caption": name,
                            "datatype": dtype,
                            "role": ""
                        })
            
            self.metadata["data_sources"].append(source_meta)
        
        # Parse worksheets
        for ws in root.findall('.//worksheet'):
            ws_meta = {
                "name": ws.get('name', ''),
                "visual_type": "unknown",
                "dimensions": [],
                "measures": [],
                "filters": []
            }
            
            # Get columns and rows
            for shelf_type in ['columns', 'rows']:
                shelf = ws.find(f'.//{shelf_type}')
                if shelf is not None:
                    for enc in shelf.findall('.//encoding'):
                        col = enc.get('column', '')
                        if col:
                            if 'sum:' in col or 'avg:' in col or ':qk' in col:
                                ws_meta["measures"].append(col)
                            else:
                                ws_meta["dimensions"].append(col)
            
            # Get filters
            for filter_node in ws.findall('.//filter'):
                ws_meta["filters"].append({
                    "column": filter_node.get('column', ''),
                    "class": filter_node.get('class', '')
                })
            
            self.metadata["worksheets"].append(ws_meta)
        
        # Parse dashboards
        for db in root.findall('.//dashboard'):
            db_meta = {
                "name": db.get('name', ''),
                "zones": []
            }
            
            for zone in db.findall('.//zone'):
                zone_name = zone.get('name', '')
                db_meta["zones"].append({
                    "name": zone_name if zone_name else None,
                    "type": zone.get('type'),
                    "x": zone.get('x', '0'),
                    "y": zone.get('y', '0'),
                    "width": zone.get('w', '50000'),
                    "height": zone.get('h', '50000')
                })
            
            self.metadata["dashboards"].append(db_meta)
    
    def _process_data_files(self):
        """Process extracted data files."""
        for file_path, extracted_path in self.data_files.items():
            if extracted_path.exists():
                print(f"   Found: {file_path}")
                
    def _convert_calculations(self):
        """Convert all Tableau calculations to DAX."""
        # Get list of all table names
        table_names = [ds.get('caption') or ds.get('name') for ds in self.metadata["data_sources"]]
        
        for calc in self.metadata["calculations"]:
            formula = calc.get('formula', '')
            ds_name = calc.get('datasource', '')
            
            # Determine the main table for this calculation
            table_name = ds_name if ds_name else "Sample - Superstore"
            
            converter = TableauToDAXConverter(table_name=table_name, all_tables=table_names)
            
            if formula:
                try:
                    dax_formula = converter.convert_formula(formula)
                    calc['dax_expression'] = dax_formula
                    print(f"   Converted: {calc.get('caption', calc['name'])}")
                except Exception as e:
                    print(f"   Warning: Could not convert {calc.get('caption', calc['name'])}: {e}")
                    calc['dax_expression'] = f"// Original: {formula}\n0"
    
    def _generate_pbip(self):
        """Generate the PBIP folder structure."""
        project_name = self.twbx_path.stem.replace(' ', '_')
        
        generator = PBIPGenerator(
            str(self.template_path),
            str(self.output_path),
            project_name
        )
        
        # Create base structure
        generator.create_structure()
        
        # Prepare tables data - deduplicate by table name
        tables = []
        seen_table_names = set()
        
        # First, add a Parameters table for storing parameter values
        parameter_calcs = [c for c in self.metadata["calculations"] if c.get('datasource') == 'Parameters']
        if parameter_calcs:
            tables.append({
                "name": "Parameters",
                "columns": [],
                "source_type": "default",
                "file_path": "",
                "sheet_name": ""
            })
            seen_table_names.add("Parameters")
        
        for ds in self.metadata["data_sources"]:
            if ds['name'] == 'Parameters':
                continue  # Handle parameters separately
                
            table_name = ds['caption'] or ds['name']
            # Clean table name
            table_name = re.sub(r'^federated\.\w+', '', table_name).strip()
            if not table_name:
                table_name = ds['name']
            
            # Skip if we've already processed this table
            if table_name in seen_table_names:
                continue
            seen_table_names.add(table_name)
            
            # Determine source type
            source_type = 'default'
            file_path = ''
            sheet_name = 'Sheet1'
            if ds.get('connection'):
                conn = ds['connection']
                if conn.get('class') == 'textscan':
                    source_type = 'csv'
                    file_path = conn.get('filename', '')
                elif conn.get('class') == 'excel-direct':
                    source_type = 'excel'
                    file_path = conn.get('filename', '')
                    sheet_name = 'Sheet1'
            
            # Special handling for main Superstore data - it uses .xls file
            if 'Superstore' in table_name and 'Sample' in table_name:
                source_type = 'excel'
                file_path = 'Data/Superstore/Sample - Superstore.xls'
                sheet_name = 'Orders'
            elif 'Sales Commission' in table_name:
                source_type = 'csv'
                file_path = 'Data/Superstore/Sales Commission.csv'
            elif 'Sales Target' in table_name:
                source_type = 'excel'
                file_path = 'Data/Superstore/Sales Target.xlsx'
                sheet_name = 'Sheet1'
            
            # Deduplicate columns
            seen_cols = set()
            unique_cols = []
            for col in ds['columns']:
                col_name = col['name']
                if col_name not in seen_cols and not col_name.startswith('[__tableau') and not col_name.startswith('__'):
                    seen_cols.add(col_name)
                    unique_cols.append(col)
            
            tables.append({
                "name": table_name,
                "columns": unique_cols,
                "source_type": source_type,
                "file_path": file_path,
                "sheet_name": sheet_name
            })
        
        # Assign measures to tables
        measures = []
        for calc in self.metadata["calculations"]:
            ds_name = calc.get('datasource', '')
            
            # Assign to Parameters table if from Parameters datasource
            if ds_name == 'Parameters':
                table_name = 'Parameters'
            else:
                # Find matching table
                table_name = None
                for t in tables:
                    if ds_name and (ds_name in t['name'] or t['name'] in ds_name):
                        table_name = t['name']
                        break
                if not table_name:
                    # Default to Sample - Superstore if available, otherwise first table
                    for t in tables:
                        if 'Superstore' in t['name']:
                            table_name = t['name']
                            break
                    if not table_name and tables:
                        table_name = tables[0]['name']
                
            measures.append({
                **calc,
                "table": table_name
            })
        
        # Generate semantic model
        generator.generate_semantic_model(tables, measures)
        
        # Generate report pages
        generator._update_definition_pbir()
        generator.generate_report_pages(
            self.metadata["worksheets"],
            self.metadata["dashboards"]
        )
        
    def _copy_data_files(self):
        """Copy data files to the output folder."""
        data_folder = self.output_path / "Data"
        data_folder.mkdir(exist_ok=True)
        
        for file_path, extracted_path in self.data_files.items():
            if extracted_path.exists():
                # Preserve subfolder structure
                rel_path = Path(file_path).relative_to('Data')
                dest = data_folder / rel_path
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(extracted_path, dest)
                print(f"   Copied: {rel_path}")
    
    def _cleanup(self):
        """Clean up temporary files."""
        if self.temp_folder.exists():
            shutil.rmtree(self.temp_folder)


def main():
    """Main entry point for the converter."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert Tableau .twbx to Power BI .pbip')
    parser.add_argument('--input', '-i', required=True, help='Path to .twbx file')
    parser.add_argument('--template', '-t', required=True, help='Path to template PBIP folder')
    parser.add_argument('--output', '-o', required=True, help='Path for output PBIP folder')
    
    args = parser.parse_args()
    
    converter = TableauToPBIPConverter(args.input, args.template, args.output)
    converter.convert()


if __name__ == "__main__":
    main()
