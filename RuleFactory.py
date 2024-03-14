import json
from Rule import Rule


class RuleFactory:
    __rules = []

    @staticmethod
    def load_rules(file: str):
        if len(RuleFactory.__rules) > 0:
            return RuleFactory.__rules
        with open(file, 'r') as f:
            rules_arr = json.load(f)
            for item in rules_arr:
                rule = Rule(item)
                RuleFactory.__rules.append(rule)
        return RuleFactory.__rules
