import pyscipopt as scip

class Staffing(object):
    N_DAYS = 7
    """
    Provide a workforce schedule for a given collection schedule.

    Given: the collection schedule (toilets and every day whether to collect or not and how much waste they accumulate) and (initially) which route/area they belong to
    Output: A list of workers per route/area-day
    """
    def __init__(self, schedule, waste, staffing_parameters):
        """

        Args:
          schedule (dataframe): For each toilet and day, collect vs not collection, and which area/route it is
          waste (dataframe): For each toilet and day, the predicted amount of waste accumulated
          parameters (dict):
            W: weight limit per worker per day
            D: day limit of workdays per week per worker
            N: number of available workers
        """
        self.schedule = schedule
        self.waste = waste
        self.parameters = staffing_parameters

    def preprocess(self):
        """
        Define the variables needed or useful for optimization
        T: number of toilets (from schedule)
        routes: ... (from schedule)
        """

    def staff(self):
        """
        Produce the staffing /workforce schedule.
        Minimize workers needed, given the constraints
        Note that the problem can be essentially decompose by area as we assume that workers don't cross areas in one day (and possibly the entire week?).

        Step 1:
          * Routes given, just assign the number of workers depending on how much waste we expect?
          * In fact, one could consider each area to be a route and it will give a number of people assigned to an area, assuming they can then determine and divide the routes among themselves.
          * Alternatively, could determine the routes by just clustering toilets, putting together toilets so that we just about fill up Worker's carry limit per route
          * Constraints on waste lifted/carried (assume the crew leader can lift and then each of the workers)
          * Use one worker at most 5 days a week?

         z_crd: collector c assigned to route r available on day d. A limited number of workers?
         gamma_td: toilet t should be collected on day d
         gamma_rd: route r should be collected on day d. gamma_rd = max_t gamma_td * chi_rt. If a route coincides with an area, then can have gamma_rd = 1 (assuming every area has some toilets to be collected?)
         lambda_rd: Collector lower bound per route... normally 2, 3 for handcarts
         chi_rt: toilet t lies on route r
         w_rd: weight (predicted) on route r on day d. w_rd = sum_t chi_rt * w_td
         W: weight limit per worker per day

         min_{z_crd} sum z_crd
         sum_rd z_crd <= 5 forall c
         sum_c z_crd >= 2*gamma_rd forall r,d #Assign 2+ workers if a route is to be collected. Needs to have at least 2, we know... for the wheelbarrow routes, need to have at least three?
         sum_c z_crd * W >= w_rd forall r, d

        Step 2:
          * Upper bound on workers and a penalty for uncollectable toilets (flexible workforce vs toilet overflows)
          * Extra constraints on worker unavailability

        Step 3:
        #TODO
          * "Routing" within areas. Consider just aerial distances between toilets. The thing is, we could consolidate some routes when we only collect occasionally.
          * Collection window constraints (because the waste accumulates when?)
          * Constraints on the distance traveled by a worker

        Step 4: Other constraints/objectives
          * Training (have workers rotate through different areas)
        """
        self.preprocess()

        s = scip.Model("Staffing")
        #s.hideOutput()
        s.setMinimize()

        #Vars: z_crd: collector c assigned to route r for day d
        for c in range(0,self.parameters['N']):
            for i_r, r in enumerate(self.routes):
                for d in range(0, N_DAYS):
                    v_name = 'z' + c + i_r + d
                    #This also handles the objective function...
                    s.addVar(v_name, vtype='B', obj=1.0)

        #Constraints


        #Objective function
        #Done: See in Vars

        s.optimize()
