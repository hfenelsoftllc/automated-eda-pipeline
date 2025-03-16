import pandas as pd
import numpy as np


class DataCleaningPipeline:
    def __init__(self):
        self.steps = []
        

    def add_step(self, name, function):
        # add steps logic
        self.steps.append({'name': name, 'function': function})

    def get_cleaned_data(self):
        return self.cleaned_data
    
    def execute(self, dtf):
        results = []
        current_dtf = dtf.copy()
        
        for step in self.steps:
            try:
                current_dtf = step['function'](current_dtf)
                results.append({
                        'name': step['name'],
                        'status': 'success',
                        'row_affected': len(current_dtf)
                    })
            except Exception as e:
                results.append({
                        'name': step['name'],
                        'status': 'failed',
                        'error': str(e)
                    })
                break
            
        return current_dtf, results
    
    def remove_duplicates(dtf):
        return dtf.drop_duplicates()
    
    def standardize_dates(dtf):
        date_columns = dtf.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            dtf[col] = pd.to_datetime(dtf[col], errors='coerce')
            return dtf