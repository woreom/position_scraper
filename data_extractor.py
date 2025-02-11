from openai import OpenAI
import json
from typing import Dict, List
import os

def clean_openai_response(response_text: str) -> str:
    """Clean OpenAI response by removing markdown formatting"""
    # Remove markdown code block indicators
    response_text = response_text.replace('```json', '').replace('```', '')
    # Remove leading/trailing whitespace
    response_text = response_text.strip()
    return response_text

def truncate_content(content: str, max_chars: int = 10000//3) -> str:
    """Truncate content by removing navigation, headers, footers, and other boilerplate"""
    if not content or len(content) <= max_chars:
        return content
        
    # Split content into lines
    lines = content.split('\n')
    
    # Skip common header/footer/navigation patterns
    skip_patterns = [
        'navigation', 'menu', 'copyright', 'footer', 'header',
        'privacy policy', 'terms of use', 'skip to content',
        'search', 'social media', 'follow us', 'contact us',
        'all rights reserved', '©', 'cookie'
    ]
    
    # Keep only relevant content lines
    filtered_lines = []
    in_main_content = False
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Skip lines that match header/footer patterns
        if any(pattern in line.lower() for pattern in skip_patterns):
            continue
            
        # Look for main content markers
        if any(marker in line.lower() for marker in ['biography', 'research', 'publications', 'about', 'profile']):
            in_main_content = True
            
        if in_main_content:
            filtered_lines.append(line)
            
        # Stop if we've collected enough content
        if len('\n'.join(filtered_lines)) >= max_chars:
            break
    
    # If we haven't found any main content markers, take the middle portion
    if not filtered_lines:
        middle_start = len(lines) // 4
        middle_end = len(lines) * 3 // 4
        filtered_lines = lines[middle_start:middle_end]
    
    # Join and truncate to max length if still needed
    result = ' '.join(filtered_lines)
    if len(result) > max_chars:
        return result[:max_chars//2] + "\n...[content truncated]...\n" + result[-max_chars//2:]
    
    return result

def extract_researcher_data(page_content: str) -> Dict:
    """Extract structured researcher data using OpenAI"""
    client = OpenAI()
    
    # Truncate content to stay within token limits
    truncated_content = truncate_content(page_content)
    
    system_prompt = """You are a research profile analyzer assessing academic profiles for PhD opportunities.
    Return ONLY a JSON object with these exact keys:
    - position (current academic position/title)
    - institute (university or research institution name)
    - department (academic department name)
    - advisor (if PhD student/postdoc, their advisor's name)
    - interests (research interests and areas)
    - email (extract any email addresses)
    - funding_likelihood (assess as "High", "Medium", or "Low" based on position, institution reputation, and research area)
    
    For funding_likelihood assessment:
    - "High": Full professors at top universities, or researchers in well-funded fields
    - "Medium": Associate professors, or professors at mid-tier institutions
    - "Low": Non-faculty positions or institutions with limited research funding"""
    
    user_prompt = f"""
    Extract information from this webpage content, paying special attention to details that indicate funding availability.
    Consider:
    - Institution's research standing and size
    - Professor's seniority and position
    - Department's prominence
    - Research field's typical funding availability
    
    Content: {truncated_content}
    
    Return all fields, ensuring funding_likelihood is "High", "Medium", or "Low" based on the above criteria.
    """
    
    try:
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",  # Using gpt-3.5-turbo for better stability
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        
        # Clean and parse response
        response_text = completion.choices[0].message.content
        cleaned_response = clean_openai_response(response_text)
        
        try:
            data = json.loads(cleaned_response)
            
            # Clean up None values and empty structures
            data = {k: v for k, v in data.items() 
                   if v and (not isinstance(v, (dict, list)) or len(v) > 0)}
            
            return data
            
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {str(e)}")
            print(f"Cleaned response: {cleaned_response[:200]}...")
            return {}
        
    except Exception as e:
        print(f"OpenAI extraction error: {str(e)}")
        return {}
