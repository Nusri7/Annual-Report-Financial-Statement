"""
Dynamic SOP Calculation Engine
Supports flexible configuration-based SOP calculations for different company types
"""

import json
import os
import pandas as pd
from typing import Dict, List, Tuple, Any, Optional

class SOPEngine:
    def __init__(self, config_path: str = None, company_type: str = None, use_knowledge_based: bool = True):
        """
        Initialize SOP Engine with configuration or knowledge-based approach
        
        Args:
            config_path: Path to specific config file (optional)
            company_type: Company type to auto-load config (optional)
            use_knowledge_based: Use AI knowledge-based extraction (default: True)
        """
        self.config = None
        self.sop_metrics = {}
        self.sop_source_terms = {}
        self.sop_calculations = {}
        self.use_knowledge_based = use_knowledge_based
        
        if not use_knowledge_based:
            if config_path and os.path.exists(config_path):
                self.load_config(config_path)
            elif company_type:
                self.load_config_by_type(company_type)
        else:
            print("🧠 SOPEngine initialized with AI knowledge-based extraction")
    
    def load_config(self, config_path: str):
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                raw_config = json.load(f)
            
            # Check if this is a company-specific format (like COMB.N.json)
            if self._is_company_specific_format(raw_config):
                # Convert company-specific format to standard format
                self.config = self._convert_company_specific_format(raw_config, config_path)
                company_name = list(raw_config.keys())[0]
                print(f"✅ Loaded company-specific SOP config for: {company_name}")
            else:
                # Standard format
                self.config = raw_config
                print(f"✅ Loaded SOP config: {self.config.get('description', 'Unknown')}")
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            raise
    
    def _is_company_specific_format(self, config: dict) -> bool:
        """Check if config is in company-specific format (e.g., {COMB.N: {...}})"""
        # Company-specific format has company name as top-level key
        # Standard format has 'company_type', 'description', 'sop_definitions' as top-level keys
        top_level_keys = set(config.keys())
        standard_keys = {'company_type', 'description', 'sop_definitions'}
        
        # If it doesn't have standard keys and has only one key, it's likely company-specific
        if not (standard_keys & top_level_keys) and len(config) == 1:
            # Check if the single key contains SOP definitions
            single_key = list(config.keys())[0]
            if isinstance(config[single_key], dict):
                return True
        return False
    
    def _convert_company_specific_format(self, raw_config: dict, config_path: str) -> dict:
        """Convert company-specific format to standard SOPEngine format"""
        company_name = list(raw_config.keys())[0]
        company_sops = raw_config[company_name]
        
        # Create standard format
        standard_config = {
            "company_type": company_name,
            "description": f"Company-specific configuration for {company_name}",
            "sop_definitions": {}
        }
        
        # Convert each SOP definition to standard format
        for sop_name, sop_def in company_sops.items():
            standard_config["sop_definitions"][sop_name] = self._convert_sop_definition(sop_def)
        
        return standard_config
    
    def _convert_sop_definition(self, sop_def: dict) -> dict:
        """Convert a single SOP definition to standard format"""
        converted = sop_def.copy()
        
        # Handle components for calculated SOPs
        if sop_def.get("type") == "calculated" and "components" in sop_def:
            components = sop_def["components"]
            
            # If components are just strings, convert to standard format
            if components and isinstance(components[0], str):
                converted_components = []
                for i, component in enumerate(components):
                    # Use the actual component name instead of generic component_1, component_2
                    component_name = component.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                    converted_components.append({
                        "name": component_name,
                        "search_terms": [component],
                        "operation": "add",
                        "required": True,
                        "display_name": component  # Store original name for display
                    })
                converted["components"] = converted_components
        
        # Handle custom formulas - convert "expression" to "formula_expression"
        if sop_def.get("type") == "calculated" and sop_def.get("formula") == "custom":
            if "expression" in sop_def and "formula_expression" not in sop_def:
                converted["formula_expression"] = sop_def["expression"]
        
        return converted
    
    def load_config_by_type(self, company_type: str):
        """Load configuration by company type"""
        config_path = f"sop_configs/{company_type}_config.json"
        if os.path.exists(config_path):
            self.load_config(config_path)
        else:
            raise FileNotFoundError(f"Config not found for company type: {company_type}")
    
    def list_available_configs(self) -> List[str]:
        """List all available configuration files"""
        configs = []
        if os.path.exists("sop_configs"):
            for file in os.listdir("sop_configs"):
                if file.endswith("_config.json"):
                    company_type = file.replace("_config.json", "")
                    configs.append(company_type)
        return configs
    
    def find_value_in_dataframes(self, search_terms: List[str], dataframes: List[pd.DataFrame], exact_match: bool = False) -> Tuple[float, str]:
        """
        Enhanced intelligent value finder with fuzzy matching and semantic understanding
        """
        # First try exact matching
        value, source_term = self._find_value_exact_match(search_terms, dataframes, exact_match)
        if value != 0:
            return value, source_term
        
        # If exact match fails, try intelligent fuzzy matching
        return self._find_value_intelligent_match(search_terms, dataframes)
    
    def _find_value_exact_match(self, search_terms: List[str], dataframes: List[pd.DataFrame], exact_match: bool = False) -> Tuple[float, str]:
        """Original exact matching logic with Group/Consolidated priority"""
        best_value = 0.0
        best_source_term = ""
        best_priority = -1
        
        for df_idx, df in enumerate(dataframes):
            if df.empty or len(df.columns) < 2:
                continue
            
            # Priority score based on dataframe position (first dataframes often more important)
            priority_score = (len(dataframes) - df_idx) * 100
            
            # Check if this is a Group/Consolidated table (higher priority)
            column_headers = ' '.join([str(col).lower() for col in df.columns])
            if 'group' in column_headers or 'consolidated' in column_headers:
                priority_score += 1000  # Boost priority for Group/Consolidated tables
            elif 'company' in column_headers:
                priority_score += 100   # Lower priority for Company tables
            
            # Look in the first column for the search terms
            first_col = df.iloc[:, 0].astype(str).str.lower()
            
            for term in search_terms:
                term_lower = term.lower()
                
                if exact_match:
                    matches = first_col == term_lower
                else:
                    matches = first_col.str.contains(term_lower, na=False, regex=False)
                
                if matches.any():
                    matched_row = df[matches].iloc[0]
                    actual_term_found = matched_row.iloc[0]
                    
                    # Get values from columns - ALWAYS prioritize the LATEST/MOST RECENT column
                    best_col_value = 0.0
                    best_col_priority = -1
                    
                    for i in range(1, len(matched_row)):
                        col_header = str(df.columns[i]).lower()
                        
                        # Skip percentage, change, and ratio columns
                        if any(skip_word in col_header for skip_word in ['%', 'change', 'ratio', 'growth']):
                            continue
                        
                        val = self.clean_numeric_value(matched_row.iloc[i])
                        if val != 0 or str(matched_row.iloc[i]).strip() in ['0', '0.0']:
                            # Determine column priority based on dates
                            col_priority = 0
                            
                            # Higher priority for more recent years (current year focus)
                            if '2025' in col_header:
                                col_priority += 2000  # Highest priority for current year
                            elif '2024' in col_header:
                                col_priority += 1000  # High priority for previous year
                            elif '2023' in col_header:
                                col_priority += 100   # Lower priority for older years
                            
                            # Higher priority for more recent quarters/months
                            if any(recent in col_header for recent in ['mar 2025', 'q1 2025', '31 mar 2025']):
                                col_priority += 2000
                            elif any(recent in col_header for recent in ['dec 2024', 'q4 2024', '31 dec 2024']):
                                col_priority += 1500
                            
                            # Use leftmost position as tiebreaker (usually more recent)
                            col_priority += (100 - i)
                            
                            if col_priority > best_col_priority:
                                best_col_value = val
                                best_col_priority = col_priority
                    
                    if best_col_value != 0:
                        current_priority = priority_score + best_col_priority
                        
                        if current_priority > best_priority:
                            best_value = best_col_value
                            best_priority = current_priority
                            best_source_term = actual_term_found
        
        return best_value, best_source_term
    
    def _find_value_intelligent_match(self, search_terms: List[str], dataframes: List[pd.DataFrame]) -> Tuple[float, str]:
        """Intelligent fuzzy matching with semantic understanding and Group/Consolidated priority"""
        best_value = 0.0
        best_source_term = ""
        best_similarity = 0.0
        
        print(f"🔍 Intelligent search for: {search_terms}")
        
        for df_idx, df in enumerate(dataframes):
            if df.empty or len(df.columns) < 2:
                continue
            
            # Check if this is a Group/Consolidated table (higher priority)
            column_headers = ' '.join([str(col).lower() for col in df.columns])
            is_group_table = 'group' in column_headers or 'consolidated' in column_headers
            is_company_table = 'company' in column_headers
            
            first_col = df.iloc[:, 0].astype(str)
            
            for row_idx, row_text in enumerate(first_col):
                if pd.isna(row_text) or not row_text.strip():
                    continue
                
                row_text_clean = str(row_text).strip()
                similarity = self._calculate_semantic_similarity(search_terms, row_text_clean)
                
                # Boost similarity for Group/Consolidated tables
                if is_group_table:
                    similarity += 0.2  # Boost for Group/Consolidated tables
                elif is_company_table:
                    similarity -= 0.1  # Reduce for Company tables
                
                if similarity > 0.6 and similarity > best_similarity:  # Threshold for similarity
                    # Extract value from this row
                    matched_row = df.iloc[row_idx]
                    value = self._extract_best_value_from_row(matched_row, df.columns)
                    
                    if value != 0:
                        best_value = value
                        best_source_term = row_text_clean
                        best_similarity = similarity
                        table_type = "Group" if is_group_table else "Company" if is_company_table else "Other"
                        print(f"  ✅ Found match: '{row_text_clean}' (similarity: {similarity:.2f}, {table_type}) = {value:,.2f}")
        
        if best_value == 0:
            print(f"  ❌ No suitable matches found for: {search_terms}")
        
        return best_value, best_source_term
    
    def _calculate_semantic_similarity(self, search_terms: List[str], target_text: str) -> float:
        """Calculate semantic similarity between search terms and target text"""
        target_lower = target_text.lower()
        max_similarity = 0.0
        
        for term in search_terms:
            term_lower = term.lower()
            
            # Exact match gets highest score
            if term_lower == target_lower:
                return 1.0
            
            # Check if target contains the term
            if term_lower in target_lower:
                similarity = 0.9
            else:
                # Check word-by-word similarity
                term_words = set(term_lower.split())
                target_words = set(target_lower.split())
                
                if term_words and target_words:
                    common_words = term_words.intersection(target_words)
                    similarity = len(common_words) / max(len(term_words), len(target_words))
                else:
                    similarity = 0.0
            
            # Boost similarity for financial terms
            similarity = self._boost_financial_similarity(term_lower, target_lower, similarity)
            
            max_similarity = max(max_similarity, similarity)
        
        return max_similarity
    
    def _boost_financial_similarity(self, term: str, target: str, base_similarity: float) -> float:
        """Boost similarity for known financial term variations"""
        
        financial_synonyms = {
            'revenue': ['income', 'earnings', 'sales', 'turnover', 'proceeds', 'total revenue', 'gross revenue'],
            'profit': ['earnings', 'income', 'surplus'],
            'net profit': ['net earnings', 'net income', 'profit for the period', 'profit for the year', 'profit/(loss) for the period'],
            'gross profit': ['gross earnings', 'gross income', 'net operating income'],
            'operating profit': ['operating income', 'operating earnings', 'ebit', 'operating profit before tax'],
            'profit before tax': ['earnings before tax', 'pre-tax profit', 'profit before taxation', 'profit/ (loss) before income tax'],
            'taxation': ['tax expense', 'income tax', 'tax provision', 'income tax expense'],
            'total assets': ['assets total', 'total asset'],
            'total liabilities': ['liabilities total', 'total liability'],
            'cash': ['cash and cash equivalents', 'cash equivalents'],
            'share price': ['price per share', 'market price', 'stock price', 'last traded', 'market price of ordinary share'],
            'earnings per share': ['eps', 'earning per share', 'basic earnings per share'],
            'depreciation': ['depreciation expense', 'depreciation cost'],
            'interest income': ['financing income', 'finance income'],
            'fee and commission income': ['net fee and commission income'],
            'net trading income': ['trading income'],
            'other operating income': ['net other operating income'],
            'total number of issued shares': ['number of shares', 'ordinary shares', 'number of ordinary shares'],
        }
        
        # Check if term has known synonyms in target
        for main_term, synonyms in financial_synonyms.items():
            if main_term in term:
                for synonym in synonyms:
                    if synonym in target:
                        return max(base_similarity, 0.8)
        
        return base_similarity
    
    def _extract_best_value_from_row(self, row: pd.Series, columns: pd.Index) -> float:
        """Extract the best value from a matched row, prioritizing recent dates"""
        best_value = 0.0
        best_priority = -1
        
        for i in range(1, len(row)):
            if i >= len(columns):
                break
                
            col_header = str(columns[i]).lower()
            
            # Skip percentage, change, and ratio columns
            if any(skip_word in col_header for skip_word in ['%', 'change', 'ratio', 'growth']):
                continue
            
            val = self.clean_numeric_value(row.iloc[i])
            if val != 0 or str(row.iloc[i]).strip() in ['0', '0.0']:
                # Determine column priority based on dates
                col_priority = 0
                
                # Higher priority for more recent years (current year focus)
                if '2025' in col_header:
                    col_priority += 2000  # Highest priority for current year
                elif '2024' in col_header:
                    col_priority += 1000  # High priority for previous year
                elif '2023' in col_header:
                    col_priority += 100   # Lower priority for older years
                
                # Higher priority for more recent quarters/months
                if any(recent in col_header for recent in ['mar 2025', 'q1 2025', '31 mar 2025']):
                    col_priority += 2000
                elif any(recent in col_header for recent in ['dec 2024', 'q4 2024', '31 dec 2024']):
                    col_priority += 1500
                
                # Use leftmost position as tiebreaker (usually more recent)
                col_priority += (100 - i)
                
                if col_priority > best_priority:
                    best_value = val
                    best_priority = col_priority
        
        return best_value
    
    def clean_numeric_value(self, value) -> float:
        """Clean and convert value to numeric, handling various formats"""
        if pd.isna(value):
            return 0.0
        
        value_str = str(value).strip()
        
        # Skip percentage values
        if '%' in value_str:
            return 0.0
        
        # Handle dashes - for calculations, treat dashes as 0.0
        # This is different from display where we want to preserve dashes
        if value_str in ['', '-', '--', '---', 'n/a', 'N/A', 'nil', 'Nil']:
            return 0.0
        
        # Remove common formatting
        cleaned = value_str.replace(',', '').replace('*', '')
        
        # Handle parentheses (negative values)
        is_negative = False
        if '(' in cleaned and ')' in cleaned:
            is_negative = True
            cleaned = cleaned.replace('(', '').replace(')', '')
        
        try:
            result = float(cleaned)
            return -result if is_negative else result
        except (ValueError, TypeError):
            return 0.0
    
    def calculate_sop_metrics(self, dataframes: List[pd.DataFrame], extracted_text: str = "") -> Tuple[Dict, Dict, Dict]:
        """
        Calculate SOP metrics using either configuration or AI knowledge-based approach
        """
        if self.use_knowledge_based:
            return self._calculate_knowledge_based_metrics(dataframes, extracted_text)
        else:
            return self._calculate_config_based_metrics(dataframes, extracted_text)
    
    def _calculate_knowledge_based_metrics(self, dataframes: List[pd.DataFrame], extracted_text: str = "") -> Tuple[Dict, Dict, Dict]:
        """
        Calculate SOP metrics using AI knowledge and understanding of financial statements
        """
        print("🧠 Calculating SOPs using AI knowledge-based approach...")
        
        # Import the knowledge-based function from main.py
        try:
            from main import extract_sop_metrics_knowledge_based
            return extract_sop_metrics_knowledge_based(
                {f"sheet_{i}": df.to_string() for i, df in enumerate(dataframes)}, 
                extracted_text
            )
        except ImportError:
            print("❌ Could not import knowledge-based extraction function")
            return {}, {}, {}
    
    def _calculate_config_based_metrics(self, dataframes: List[pd.DataFrame], extracted_text: str = "") -> Tuple[Dict, Dict, Dict]:
        """
        Calculate SOP metrics based on the loaded configuration (legacy method)
        """
        if not self.config:
            raise ValueError("No configuration loaded. Please load a config first.")
        
        self.sop_metrics = {}
        self.sop_source_terms = {}
        self.sop_calculations = {}
        
        sop_definitions = self.config.get('sop_definitions', {})
        
        print(f"📊 Calculating SOPs using {self.config.get('company_type', 'unknown')} configuration...")
        
        # Process each SOP definition
        for sop_name, sop_config in sop_definitions.items():
            try:
                value, source_term, calculation = self._process_sop_definition(sop_name, sop_config, dataframes, extracted_text)
                
                self.sop_metrics[sop_name] = value
                self.sop_source_terms[sop_name] = source_term
                self.sop_calculations[sop_name] = calculation
                
                if value != 0:
                    print(f"✅ {sop_name}: {value:,.2f} ({source_term})")
                else:
                    print(f"❌ {sop_name}: Not found/calculated")
                    
            except Exception as e:
                print(f"⚠️ Error processing {sop_name}: {e}")
                self.sop_metrics[sop_name] = 0.0
                self.sop_source_terms[sop_name] = f"Error: {str(e)}"
                self.sop_calculations[sop_name] = "Failed to calculate"
        
        return self.sop_metrics, self.sop_source_terms, self.sop_calculations
    
    def _process_sop_definition(self, sop_name: str, sop_config: Dict, dataframes: List[pd.DataFrame], extracted_text: str) -> Tuple[float, str, str]:
        """Process a single SOP definition"""
        sop_type = sop_config.get('type', 'direct')
        
        if sop_type == 'direct':
            return self._process_direct_sop(sop_config, dataframes)
        
        elif sop_type == 'calculated':
            return self._process_calculated_sop(sop_config, dataframes)
        
        elif sop_type == 'forced_value':
            value = sop_config.get('value', 0.0)
            reason = sop_config.get('reason', 'Forced value')
            return value, reason, f"Forced to {value}"
        
        elif sop_type == 'manual_input':
            description = sop_config.get('description', 'Manual input required')
            print(f"⚠️ Manual input required for {sop_name}: {description}")
            return 0.0, "Manual input required", description
        
        elif sop_type == 'text_search':
            return self._process_text_search_sop(sop_config, dataframes, extracted_text)
        
        else:
            raise ValueError(f"Unknown SOP type: {sop_type}")
    
    def _process_direct_sop(self, sop_config: Dict, dataframes: List[pd.DataFrame]) -> Tuple[float, str, str]:
        """Process direct extraction SOP"""
        search_terms = sop_config.get('search_terms', [])
        value, source_term = self.find_value_in_dataframes(search_terms, dataframes)
        
        return value, source_term if source_term else "Not found", "Direct extraction"
    
    def _process_text_search_sop(self, sop_config: Dict, dataframes: List[pd.DataFrame], extracted_text: str) -> Tuple[float, str, str]:
        """Process SOP that might require text file search"""
        search_terms = sop_config.get('search_terms', [])
        
        # First try to find in dataframes
        value, source_term = self.find_value_in_dataframes(search_terms, dataframes)
        
        if value != 0:
            return value, source_term, "Direct extraction from Excel"
        
        # If not found in Excel and text search is enabled, search in text
        if extracted_text and sop_config.get('fallback_to_text', False):
            text_value, text_source = self._search_in_text(search_terms, extracted_text)
            if text_value != 0:
                return text_value, text_source, "Extracted from text file"
        
        return 0.0, "Not found in Excel or text", "Text search attempted"
    
    def _search_in_text(self, search_terms: List[str], text: str) -> Tuple[float, str]:
        """Search for values in extracted text using regex patterns"""
        import re
        
        for term in search_terms:
            # Create regex patterns to find the term followed by a value
            patterns = [
                # Pattern 1: "Term: Value" or "Term Value"
                rf"{re.escape(term)}\s*:?\s*([0-9,]+\.?[0-9]*)",
                # Pattern 2: "Term (Rs) Value" 
                rf"{re.escape(term)}\s*\([^)]*\)\s*([0-9,]+\.?[0-9]*)",
                # Pattern 3: More flexible pattern
                rf"{re.escape(term)}[:\s]*([0-9,]+\.?[0-9]*)"
            ]
            
            for pattern in patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    try:
                        value_str = match.group(1).replace(',', '')
                        value = float(value_str)
                        if value > 0:  # Only return positive values
                            return value, f"Found '{term}' in text"
                    except (ValueError, IndexError):
                        continue
        
        return 0.0, "Not found in text"
    
    def _process_calculated_sop(self, sop_config: Dict, dataframes: List[pd.DataFrame]) -> Tuple[float, str, str]:
        """Process calculated SOP"""
        formula = sop_config.get('formula', 'sum')
        components = sop_config.get('components', [])
        
        if formula == 'sum':
            return self._calculate_sum(components, dataframes)
        
        elif formula == 'subtract':
            return self._calculate_subtract(components, dataframes)
        
        elif formula == 'multiply':
            return self._calculate_multiply(components, dataframes)
        
        elif formula == 'divide':
            return self._calculate_divide(components, dataframes)
        
        elif formula == 'percentage':
            formula_expression = sop_config.get('formula_expression', '')
            return self._calculate_percentage(formula_expression)
        
        elif formula == 'custom':
            formula_expression = sop_config.get('formula_expression', '')
            return self._calculate_custom(formula_expression)
        
        elif formula == 'custom_net_borrowings':
            return self._calculate_net_borrowings(components, dataframes)
        
        else:
            raise ValueError(f"Unknown formula type: {formula}")
    
    def _calculate_sum(self, components: List[Dict], dataframes: List[pd.DataFrame]) -> Tuple[float, str, str]:
        """Calculate sum of components"""
        total = 0.0
        found_components = []
        
        for component in components:
            value = self._get_component_value(component, dataframes)
            operation = component.get('operation', 'add')
            
            # Use display_name if available, otherwise fall back to name
            component_display_name = component.get('display_name', component.get('name', 'unknown'))
            
            if operation == 'add':
                total += value
                if value != 0:
                    found_components.append(f"{component_display_name}: +{value:,.0f}")
            
            elif operation == 'add_if_positive_subtract_if_negative':
                if value < 0:
                    total += value  # Adding negative = subtraction
                    found_components.append(f"{component_display_name}: {value:,.0f} (LOSS)")
                elif value > 0:
                    total += value
                    found_components.append(f"{component_display_name}: +{value:,.0f} (GAIN)")
            
            elif operation == 'add_absolute':
                total += abs(value)
                if value != 0:
                    found_components.append(f"{component_display_name}: +{abs(value):,.0f}")
        
        calculation_detail = " + ".join(found_components) if found_components else "No components found"
        return total, "Calculated from components", calculation_detail
    
    def _calculate_subtract(self, components: List[Dict], dataframes: List[pd.DataFrame]) -> Tuple[float, str, str]:
        """Calculate subtraction of components"""
        result = None
        calculation_parts = []
        
        for component in components:
            value = self._get_component_value(component, dataframes)
            operation = component.get('operation', 'add')
            
            # Use display_name if available, otherwise fall back to name
            component_display_name = component.get('display_name', component.get('name', 'unknown'))
            
            if result is None:
                result = value
                calculation_parts.append(f"{component_display_name}: {value:,.0f}")
            else:
                if operation == 'subtract':
                    result -= value
                    calculation_parts.append(f"- {component_display_name}: {value:,.0f}")
                else:
                    result += value
                    calculation_parts.append(f"+ {component_display_name}: {value:,.0f}")
        
        calculation_detail = " ".join(calculation_parts) if calculation_parts else "No components found"
        return result if result is not None else 0.0, "Calculated by subtraction", calculation_detail
    
    def _calculate_multiply(self, components: List[Dict], dataframes: List[pd.DataFrame]) -> Tuple[float, str, str]:
        """Calculate multiplication of components"""
        result = 1.0
        calculation_parts = []
        
        for component in components:
            value = self._get_component_value(component, dataframes)
            result *= value
            # Use display_name if available, otherwise fall back to name
            component_display_name = component.get('display_name', component.get('name', 'unknown'))
            calculation_parts.append(f"{component_display_name}: {value:,.0f}")
        
        calculation_detail = " × ".join(calculation_parts) if calculation_parts else "No components found"
        return result, "Calculated by multiplication", calculation_detail
    
    def _calculate_divide(self, components: List[Dict], dataframes: List[pd.DataFrame]) -> Tuple[float, str, str]:
        """Calculate division of components"""
        result = None
        calculation_parts = []
        
        for component in components:
            value = self._get_component_value(component, dataframes)
            
            # Use display_name if available, otherwise fall back to name
            component_display_name = component.get('display_name', component.get('name', 'unknown'))
            
            if result is None:
                result = value
                calculation_parts.append(f"{component_display_name}: {value:,.0f}")
            else:
                if value != 0:
                    result /= value
                    calculation_parts.append(f"÷ {component_display_name}: {value:,.0f}")
                else:
                    result = 0.0
                    calculation_parts.append(f"÷ {component_display_name}: 0 (division by zero)")
        
        calculation_detail = " ".join(calculation_parts) if calculation_parts else "No components found"
        return result if result is not None else 0.0, "Calculated by division", calculation_detail
    
    def _calculate_percentage(self, formula_expression: str) -> Tuple[float, str, str]:
        """Calculate percentage formula"""
        try:
            # Parse expressions like "(Taxation / Revenue) * 100"
            if "Taxation" in formula_expression and "Revenue" in formula_expression:
                taxation = self.sop_metrics.get('Taxation', 0)
                revenue = self.sop_metrics.get('Revenue', 0)
                if revenue != 0:
                    result = (abs(taxation) / revenue) * 100
                    return result, "Calculated percentage", f"({abs(taxation):,.2f} / {revenue:,.2f}) × 100 = {result:.2f}%"
                else:
                    return 0.0, "Division by zero", "Revenue is 0"
            
            elif "Taxation" in formula_expression and "Profit Before Tax" in formula_expression:
                taxation = self.sop_metrics.get('Taxation', 0)
                pbt = self.sop_metrics.get('Profit Before Tax', 0)
                if pbt != 0:
                    result = (abs(taxation) / pbt) * 100
                    return result, "Calculated percentage", f"({abs(taxation):,.2f} / {pbt:,.2f}) × 100 = {result:.2f}%"
                else:
                    return 0.0, "Division by zero", "Profit Before Tax is 0"
            
            return 0.0, "Unsupported formula", formula_expression
        except Exception as e:
            return 0.0, f"Calculation error: {e}", formula_expression
    
    def _calculate_custom(self, formula_expression: str) -> Tuple[float, str, str]:
        """Calculate custom formula by parsing and evaluating expressions"""
        try:
            return self._parse_and_evaluate_formula(formula_expression)
        except Exception as e:
            return 0.0, f"Custom calculation error: {e}", formula_expression
    
    def _parse_and_evaluate_formula(self, formula_expression: str) -> Tuple[float, str, str]:
        """Parse and evaluate a formula expression with SOP metric names"""
        import re
        
        # Store original formula for display
        original_formula = formula_expression
        
        # Find all potential SOP metric names in the formula
        # Look for words/phrases that could be metric names
        potential_metrics = []
        
        # Common SOP metric patterns
        metric_patterns = [
            r'\b(?:Market Capitalization|Total Debt|Cash and Cash Equivalents|Cash)\b',
            r'\b(?:Total Tax Paid|Total Income|Income Tax Expense|Earnings Before Taxes \(EBT\))\b',
            r'\b(?:Taxation|Revenue|Profit Before Tax)\b',
            r'\b(?:[A-Z][a-zA-Z\s\(\)]+)\b'  # General pattern for capitalized metric names
        ]
        
        substituted_formula = formula_expression
        substitution_map = {}
        calculation_parts = []
        
        # First try to match specific known patterns
        known_substitutions = {
            'Market Capitalization': 'Market Capitalization',
            'Total Debt': 'Total Debt', 
            'Cash and Cash Equivalents': 'Cash',
            'Cash': 'Cash',
            'Total Tax Paid': 'Taxation',
            'Total Income': 'Revenue',
            'Income Tax Expense': 'Taxation',
            'Earnings Before Taxes (EBT)': 'Profit Before Tax',
            'Taxation': 'Taxation',
            'Revenue': 'Revenue',
            'Profit Before Tax': 'Profit Before Tax',
            'Share Price': 'Share Price',
            'Total Number of Issued Shares': 'Total Number of Issued Shares'
        }
        
        # Replace known metric names with their values
        for formula_name, metric_name in known_substitutions.items():
            if formula_name in substituted_formula:
                value = self.sop_metrics.get(metric_name, 0)
                substituted_formula = substituted_formula.replace(formula_name, str(value))
                substitution_map[formula_name] = value
                calculation_parts.append(f"{formula_name} = {value:,.2f}")
        
        # Clean up the formula for evaluation
        # Only allow numbers, operators, parentheses, and decimal points
        safe_formula = re.sub(r'[^0-9+\-*/().\s]', '', substituted_formula)
        
        # Evaluate the formula
        try:
            result = eval(safe_formula)
            
            # Create detailed calculation string
            calculation_detail = f"Formula: {original_formula}"
            if calculation_parts:
                calculation_detail += f" | Substitutions: {', '.join(calculation_parts)}"
            calculation_detail += f" | Evaluated: {safe_formula} = {result:,.2f}"
            
            return float(result), "Custom formula calculation", calculation_detail
            
        except Exception as eval_error:
            # If evaluation fails, return detailed error
            return 0.0, f"Formula evaluation failed: {eval_error}", f"Original: {original_formula} | Processed: {safe_formula}"
    
    def _calculate_net_borrowings(self, components: List[Dict], dataframes: List[pd.DataFrame]) -> Tuple[float, str, str]:
        """Calculate Net Borrowings: Short term borrowings ± Long term borrowings − Principal element of lease payment"""
        net_borrowings = 0.0
        calculation_parts = []
        
        for component in components:
            value = self._get_component_value(component, dataframes)
            operation = component.get('operation', 'add')
            component_name = component.get('name', 'unknown')
            
            if operation == 'add':
                net_borrowings += value
                if value != 0:
                    calculation_parts.append(f"{component_name}: +{value:,.0f}")
            elif operation == 'subtract':
                net_borrowings -= value
                if value != 0:
                    calculation_parts.append(f"{component_name}: -{value:,.0f}")
        
        calculation_detail = " ".join(calculation_parts) if calculation_parts else "No components found"
        return net_borrowings, "Custom Net Borrowings calculation", calculation_detail
    
    def _get_component_value(self, component: Dict, dataframes: List[pd.DataFrame]) -> float:
        """Get value for a component (either from search terms or reference to another SOP)"""
        if 'reference' in component:
            # Reference to another SOP metric
            ref_name = component['reference']
            return self.sop_metrics.get(ref_name, 0.0)
        
        elif 'search_terms' in component:
            # Search in dataframes
            search_terms = component['search_terms']
            value, _ = self.find_value_in_dataframes(search_terms, dataframes)
            return value
        
        else:
            return 0.0
    
    def export_config_template(self, company_type: str, output_path: str):
        """Export a template configuration for customization"""
        template = {
            "company_type": company_type,
            "description": f"Configuration for {company_type} companies",
            "sop_definitions": {
                "Revenue": {
                    "type": "direct",
                    "search_terms": ["revenue", "total revenue"],
                    "required": True
                },
                "Net Profit": {
                    "type": "direct", 
                    "search_terms": ["net profit", "profit for the period"],
                    "required": True
                }
                # Add more template SOPs as needed
            }
        }
        
        with open(output_path, 'w') as f:
            json.dump(template, f, indent=2)
        
        print(f"✅ Template configuration exported to: {output_path}")

# Example usage
if __name__ == "__main__":
    # Initialize for bank
    engine = SOPEngine(company_type="bank")
    
    # List available configs
    print("Available configurations:", engine.list_available_configs())
