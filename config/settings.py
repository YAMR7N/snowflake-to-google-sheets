"""Global pipeline settings"""
import os
from pathlib import Path

MODELS = {
    "gpt-4o": {
        "provider": "openai", 
        "temperature": 0.0,
        # Increased timeout for FTR's XML3D format
        "timeout_seconds": 120.0,
        "max_retries": 8
    },
    "gpt-5": {
        "provider": "openai",
        "temperature": 0.0,
        # Conservative defaults; adjust if needed per workload
        "timeout_seconds": 120.0,
        "max_retries": 8
    },
    "o4-mini": {"provider": "openai", "temperature": 0.0},
    "gpt-4o-mini": {"provider": "openai", "temperature": 0.0},
    "gemini-1.5-pro": {
        "provider": "gemini", 
        "temperature": 0.0,
        # Retry configuration (optional - defaults shown)
        "max_retries": 6,
        "base_delay": 1.0,
        "max_delay": 30.0,
        "timeout_seconds": 60.0
    },
    "gemini-1.5-flash": {
        "provider": "gemini", 
        "temperature": 0.0,
        # Using default retry settings
    },
    "gemini-2.0-flash-exp": {
        "provider": "gemini", 
        "temperature": 0.0,
        # Example of more aggressive retry settings
        "max_retries": 10,
        "base_delay": 2.0,
        "timeout_seconds": 90.0
    },
    "gemini-2.5-pro": {"provider": "gemini", "temperature": 0.2},
    "gemini-2.5-flash": {
        "provider": "gemini", 
        "temperature": 0.2,
        "top_p": 1.0,
        "top_k": 40,
        "enable_thinking": False
    },
    # Anthropic models
    "claude-3-opus-20240229": {
        "provider": "anthropic",
        "temperature": 0.0,
        "max_retries": 6,
        "base_delay": 1.0,
        "max_delay": 30.0,
        "timeout_seconds": 60.0
    },
    "claude-3-sonnet-20240229": {
        "provider": "anthropic",
        "temperature": 0.0
    },
    "claude-3-haiku-20240307": {
        "provider": "anthropic",
        "temperature": 0.0
    },
    "claude-3-5-sonnet-20241022": {
        "provider": "anthropic",
        "temperature": 0.0,
        "max_retries": 10,  # More retries for the newest model
        "timeout_seconds": 90.0
    }
}

PROCESSING = {
    "max_concurrent_requests": 40,
    "retry_attempts": 3,
    "retry_delay": 2,
    "request_timeout": 60
}

DATA_PROCESSING = {
    "days_lookback": {"default": 1, "fcr": 3},
    "required_headers": ['Conversation ID', 'Customer Name', 'Message Sent Time', 'Sent By', 'TEXT', 'Skill', 'Agent Name', 'Message Type', "Tools", "Tool Creation Date", "Tools Json Output", "Tool SUCCESS"]
}

PATHS = {
    "outputs": "outputs",
    "tableau_exports": "outputs/tableau_exports",
    "preprocessing_output": "outputs/preprocessing_output", 
    "llm_outputs": "outputs/LLM_outputs",
    "rule_breaking": "outputs/rule_breaking",
    "credentials": "credentials.json"
}

FORMATS = ["json", "segmented", "transparent", "xml", "xml3d"]

class Settings:
    def __init__(self):
        self.models = MODELS
        self.processing = PROCESSING
        self.data_processing = DATA_PROCESSING
        self.paths = PATHS
        self.formats = FORMATS
        for path in self.paths.values():
            if not path.endswith('.json'):
                Path(path).mkdir(parents=True, exist_ok=True)
    
    def get_model_config(self, model_name):
        if model_name not in self.models:
            raise ValueError(f"Unknown model: {model_name}")
        return self.models[model_name]
    
    def get_days_lookback(self, prompt_type="default"):
        return self.data_processing["days_lookback"].get(prompt_type, self.data_processing["days_lookback"]["default"])
    
    def validate_format(self, format_name):
        if format_name not in self.formats:
            raise ValueError(f"Unsupported format: {format_name}")
        return True
    
    def get_env_var(self, var_name):
        value = os.getenv(var_name)
        if not value:
            raise ValueError(f"Required environment variable not set: {var_name}")
        return value

SETTINGS = Settings()
