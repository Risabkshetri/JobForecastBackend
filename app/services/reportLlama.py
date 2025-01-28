# from langchain_community.llms import Ollama
# from langchain.prompts import PromptTemplate
# from langchain.chains import LLMChain
# from app.config import settings
# from typing import List, Optional, Dict, Any
# from datetime import datetime
# import json
# import re

# class LLMService:
#     def __init__(self):
#         self.llm = Ollama(
#             model=settings.MODEL_NAME,
#             temperature=0.7,
#             base_url=settings.OLLAMA_BASE_URL,
#         )
        
#         self.prompt = PromptTemplate(
#             input_variables=[
#                 "industry", 
#                 "country",
#                 "region",
#                 "city", 
#                 "start_date", 
#                 "end_date", 
#                 "skills",
#                 "experience_level",
#                 "employment_type"
#             ],
#             template="""
#             You are an expert job market analyst with extensive experience in workforce analytics and industry trends. 
#             Analyze the job market then give me the 200 words of report by considering the following factors:

#             INPUT PARAMETERS:
#             Industry: {industry}
#             Location: {country}, {region}, {city}
#             Period: {start_date} to {end_date}
#             Skills: {skills}
#             Level: {experience_level}
#             Type: {employment_type}

#             IMPORTANT: Your response must be a valid JSON object with exactly the following structure. Do not include any additional text or explanations outside the JSON object:
#             {{
#             "report": "give persolized report here

#             Ensure the response is valid JSON.
#             """
#         )
        
#         self.chain = LLMChain(llm=self.llm, prompt=self.prompt)

#     async def generate_report(
#         self,
#         industry: str,
#         country: str,
#         region: str,
#         city: Optional[str] = None,
#         start_date: Optional[str] = None,
#         end_date: Optional[str] = None,
#         skills: Optional[List[str]] = None,
#         experience_level: Optional[str] = "Mid",
#         employment_type: Optional[str] = "Full-time"
#     ) -> Dict:
#         # Set default dates if not provided
#         if not start_date:
#             start_date = datetime.now().strftime("%Y-%m-%d")
#         if not end_date:
#             end_date = (datetime.now().replace(year=datetime.now().year + 1)).strftime("%Y-%m-%d")

#         try:
#             input_dict = {
#                 "industry": industry,
#                 "country": country,
#                 "region": region,
#                 "city": city,
#                 "start_date": start_date,
#                 "end_date": end_date,
#                 "skills": skills,
#                 "experience_level": experience_level,
#                 "employment_type": employment_type
#             }
#             response = await self.chain.ainvoke(input_dict)
#             parsed_response = self._parse_response(response['text'])
            
#             # Add metadata
#             parsed_response["metadata"] = {
#                 "analysisTimestamp": datetime.now().isoformat(),
#                 "dataFreshness": datetime.now().strftime("%Y-%m-%d"),
#                 "geographicScope": f"{region}, {country}",
#                 "industrySegment": industry
#             }
#             snakeCased_response = self._camel_to_snake_case(parsed_response)
#             return snakeCased_response
            
#         except Exception as e:
#             raise Exception(f"Error generating report: {str(e)}")

#      # Utility to convert camelCase to snake_case
#     def _camel_to_snake_case(self,data: Any) -> Any:
#         if isinstance(data, dict):
#             return {re.sub(r'(?<!^)(?=[A-Z])', '_', k).lower(): self._camel_to_snake_case(v) for k, v in data.items()}
#         elif isinstance(data, list):
#             return [self._camel_to_snake_case(item) for item in data]
#         else:
#             return data
        
#     def _parse_response(self, response: str) -> Dict:
#         try:
#             # Find the start of the JSON object
#             start_idx = response.find('{')
#             end_idx = response.rfind('}')

#             if start_idx == -1 or end_idx == -1:
#                 # If no JSON found, try to generate a default structure
#                 return self._generate_default_response()
        
#             json_str = response[start_idx:end_idx + 1]
#             try:
#                 parsed_data = json.loads(json_str)
#             except json.JSONDecodeError:
#                 # Clean up common JSON formatting issues
#                 json_str = json_str.replace('\n', '').replace('\\', '')
#                 parsed_data = json.loads(json_str)
        
#             # Ensure all required fields exist
#             parsed_data = self._ensure_required_fields(parsed_data)
        
#             return parsed_data
        
#         except Exception as e:
#             raise Exception(f"Error parsing response: {str(e)}")

#     def _ensure_required_fields(self, data: Dict) -> Dict:
#         """Ensure all required fields exist with default values if missing"""
#         default_structure = {
#             "report": "No report available"
                
#         }
#         # Merge the actual response with default structure
#         for key, default_value in default_structure.items():
#             if key not in data:
#                 data[key] = default_value
#             elif isinstance(default_value, dict):
#                 for sub_key, sub_value in default_value.items():
#                     if sub_key not in data[key]:
#                         data[key][sub_key] = sub_value

#         return data

#     def _validate_response(self, data: Dict) -> None:
#         """Validate the response data structure and types."""
#         required_sections = [
#             "report"
#         ]
        
#         for section in required_sections:
#             if section not in data:
#                 raise ValueError(f"Missing required section: {section}")
    
#         if not isinstance(data["report"], str):
#             raise ValueError("Report must be a string")

from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from app.config import settings
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import re

class LLMService:
    def __init__(self):
        self.llm = Ollama(
            model=settings.MODEL_NAME,
            temperature=0.7,
            base_url=settings.OLLAMA_BASE_URL,
        )
        
        self.prompt = PromptTemplate(
            input_variables=[
                "industry", 
                "country",
                "region",
                "city", 
                "start_date", 
                "end_date", 
                "skills",
                "experience_level",
                "employment_type"
            ],
            template=
                """
                    You are an expert job market analyst with extensive experience in workforce analytics and industry trends. 
                    Analyze the job market then give me the 200 words of report by considering the following factors:
                    
                    INPUT PARAMETERS:
                    Industry: {industry}
                    Location: {country}, {region}, {city}
                    Period: {start_date} to {end_date}
                    Skills: {skills}
                    Level: {experience_level}
                    Type: {employment_type}
                    
                    IMPORTANT: Your response must be a valid JSON object with exactly the following structure. Do not include any additional text or explanations outside the JSON object:
                    
                    {{
                        "report": "Your 200-word personalized market analysis report goes here. Focus on trends, demand patterns, and key insights relevant to the specified parameters."
                    }}
                    """
                )
        
        self.chain = LLMChain(llm=self.llm, prompt=self.prompt)

    async def generate_report(
        self,
        industry: str,
        country: str,
        region: str,
        city: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        skills: Optional[List[str]] = None,
        experience_level: Optional[str] = "Mid",
        employment_type: Optional[str] = "Full-time"
    ) -> Dict:
        # Set default dates if not provided
        if not start_date:
            start_date = datetime.now().strftime("%Y-%m-%d")
        if not end_date:
            end_date = (datetime.now().replace(year=datetime.now().year + 1)).strftime("%Y-%m-%d")

        try:
            input_dict = {
                "industry": industry,
                "country": country,
                "region": region,
                "city": city or "Not specified",
                "start_date": start_date,
                "end_date": end_date,
                "skills": skills or ["Not specified"],
                "experience_level": experience_level,
                "employment_type": employment_type
            }
            response = await self.chain.ainvoke(input_dict)
            parsed_response = self._parse_response(response['text'])
            
            # Add metadata
            parsed_response["metadata"] = {
                "analysis_timestamp": datetime.now().isoformat(),
                "data_freshness": datetime.now().strftime("%Y-%m-%d"),
                "geographic_scope": f"{region}, {country}",
                "industry_segment": industry
            }
            return parsed_response
            
        except Exception as e:
            raise Exception(f"Error generating report: {str(e)}")

    def _parse_response(self, response: str) -> Dict:
        try:
            # Find the start of the JSON object
            start_idx = response.find('{')
            end_idx = response.rfind('}')

            if start_idx == -1 or end_idx == -1:
                return self._ensure_required_fields({})
        
            json_str = response[start_idx:end_idx + 1]
            try:
                parsed_data = json.loads(json_str)
            except json.JSONDecodeError:
                # Clean up common JSON formatting issues
                json_str = json_str.replace('\n', ' ').replace('\\', '')
                parsed_data = json.loads(json_str)
        
            # Ensure all required fields exist
            parsed_data = self._ensure_required_fields(parsed_data)
            self._validate_response(parsed_data)
            return parsed_data
        
        except Exception as e:
            raise Exception(f"Error parsing response: {str(e)}")

    def _ensure_required_fields(self, data: Dict) -> Dict:
        """Ensure all required fields exist with default values if missing"""
        default_structure = {
            "report": "No report available",
            "metadata": {
                "analysis_timestamp": datetime.now().isoformat(),
                "data_freshness": datetime.now().strftime("%Y-%m-%d"),
                "geographic_scope": "Not specified",
                "industry_segment": "Not specified"
            }
        }
        # Merge the actual response with default structure
        for key, default_value in default_structure.items():
            if key not in data:
                data[key] = default_value
            elif isinstance(default_value, dict):
                if not isinstance(data[key], dict):
                    data[key] = default_value
                else:
                    for sub_key, sub_value in default_value.items():
                        if sub_key not in data[key]:
                            data[key][sub_key] = sub_value

        return data

    def _validate_response(self, data: Dict) -> None:
        """Validate the response data structure and types."""
        required_sections = ["report", "metadata"]
        
        for section in required_sections:
            if section not in data:
                raise ValueError(f"Missing required section: {section}")
    
        if not isinstance(data["report"], str):
            raise ValueError("Report must be a string")