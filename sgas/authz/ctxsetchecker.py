"""
Authorization set context checker.

Author: Henrik Thostrup Jensen

Copyright: Nordic Data Grid Facility (2010)
"""



class SetChecker:


    def __init__(self, operator):

        self.operator = operator


    def contextCheck(self, subject_identity, subject_rights, action_context):

        for ctx in subject_rights:
            ctx_allow = []
            for cak, cav in ctx.items():
                #print "CAK", cak, cav, action_context
                found_match = False
                for cik, civ in action_context:
                    if cak == cik:
                        found_match = True
                        if civ in cav:
                            ctx_allow.append(True)
                        else:
                            ctx_allow.append(False)

                if not found_match:
                    ctx_allow.append(False)

            allowed = self.operator(ctx_allow or [False])
            if allowed:
                return True

        return False



AnySetChecker = SetChecker(any)
AllSetChecker = SetChecker(all)


