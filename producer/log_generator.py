"""
Log Generator Module
Generates realistic log entries for different services
"""

import random
from datetime import datetime
from faker import Faker
import json

fake = Faker()


class LogGenerator:
    """Generate structured logs with realistic data"""
    
    def __init__(self, config):
        self.config = config
        self.log_levels = list(config['log_generation']['level_distribution'].keys())
        self.level_weights = list(config['log_generation']['level_distribution'].values())
    
    def generate_log(self, service_name, service_config):
        """Generate a single log entry for a service"""
        level = random.choices(self.log_levels, weights=self.level_weights)[0]
        
        # Generate service-specific log
        if service_name == 'web_server':
            return self._generate_web_server_log(level, service_config)
        elif service_name == 'database':
            return self._generate_database_log(level, service_config)
        elif service_name == 'api_gateway':
            return self._generate_api_gateway_log(level, service_config)
        elif service_name == 'auth_service':
            return self._generate_auth_service_log(level, service_config)
        else:
            return self._generate_generic_log(level, service_name)
    
    def _generate_web_server_log(self, level, config):
        """Generate web server log"""
        endpoint = random.choice(config['endpoints'])
        status_code = random.choice(config['status_codes'])
        latency_ms = random.randint(10, 2000)
        
        messages = {
            'DEBUG': f"Processing request to {endpoint}",
            'INFO': f"Request completed: {endpoint} - {status_code} ({latency_ms}ms)",
            'WARNING': f"Slow response on {endpoint} - {latency_ms}ms",
            'ERROR': f"Request failed: {endpoint} - {status_code}",
            'CRITICAL': f"Service unavailable for {endpoint}"
        }
        
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'service': 'web-server',
            'message': messages.get(level, messages['INFO']),
            'metadata': {
                'endpoint': endpoint,
                'status_code': status_code,
                'latency_ms': latency_ms,
                'request_id': fake.uuid4(),
                'user_id': fake.uuid4() if random.random() > 0.3 else None,
                'ip_address': fake.ipv4()
            }
        }
    
    def _generate_database_log(self, level, config):
        """Generate database log"""
        operation = random.choice(config['operations'])
        table = random.choice(config['tables'])
        execution_time = random.randint(5, 500)
        
        messages = {
            'DEBUG': f"Executing {operation} on {table}",
            'INFO': f"{operation} query completed on {table} ({execution_time}ms)",
            'WARNING': f"Slow query detected: {operation} on {table} - {execution_time}ms",
            'ERROR': f"Query failed: {operation} on {table}",
            'CRITICAL': f"Database connection lost during {operation}"
        }
        
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'service': 'database',
            'message': messages.get(level, messages['INFO']),
            'metadata': {
                'operation': operation,
                'table': table,
                'execution_time_ms': execution_time,
                'rows_affected': random.randint(0, 100) if operation != 'SELECT' else None,
                'connection_pool_size': random.randint(5, 20)
            }
        }
    
    def _generate_api_gateway_log(self, level, config):
        """Generate API gateway log"""
        route = random.choice(config['routes'])
        
        messages = {
            'DEBUG': f"Routing request to {route} service",
            'INFO': f"Request routed successfully to {route}",
            'WARNING': f"Rate limit approaching for {route}",
            'ERROR': f"Failed to route to {route} - service unavailable",
            'CRITICAL': f"Gateway overload - dropping requests to {route}"
        }
        
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'service': 'api-gateway',
            'message': messages.get(level, messages['INFO']),
            'metadata': {
                'route': route,
                'request_id': fake.uuid4(),
                'client_id': fake.uuid4(),
                'rate_limit_remaining': random.randint(0, 1000)
            }
        }
    
    def _generate_auth_service_log(self, level, config):
        """Generate authentication service log"""
        event = random.choice(config['events'])
        
        messages = {
            'DEBUG': f"Processing {event} request",
            'INFO': f"{event.replace('_', ' ').title()} successful",
            'WARNING': f"Multiple {event} attempts detected",
            'ERROR': f"{event.replace('_', ' ').title()} failed - invalid credentials",
            'CRITICAL': f"Potential security breach - suspicious {event} pattern"
        }
        
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'service': 'auth-service',
            'message': messages.get(level, messages['INFO']),
            'metadata': {
                'event': event,
                'user_id': fake.uuid4(),
                'ip_address': fake.ipv4(),
                'user_agent': fake.user_agent(),
                'session_id': fake.uuid4()
            }
        }
    
    def _generate_generic_log(self, level, service_name):
        """Generate generic log for unknown service"""
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'service': service_name,
            'message': f"{level} message from {service_name}",
            'metadata': {
                'request_id': fake.uuid4()
            }
        }
    
    def to_json(self, log_entry):
        """Convert log entry to JSON string"""
        return json.dumps(log_entry)
