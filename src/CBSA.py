class CBSA:
    """
    CBSA is used to store and operate the data of each Core Based Statistical Area
    """
    def __init__(self):
        """
        cbsa_dict stores data of each Core Based Statistical Area
        key: cbsa09
        value: [cbsa_t, total_pop_00, total_pop_10, pop_growth_rate]
        """
        self.cbsa_dict = {}
    
    def has_cbsa09(self, cbsa09):
        """
        return a boolean indicating if CBSA contains an Core Based Statistical Area Code
        @cbsa09: Core Based Statistical Area Code
        """
        return cbsa09 in self.cbsa_dict
    
    def new_cbsa_t(self, cbsa09, cbsa_t):
        """
        create a new entry
        @cbsa09: Core Based Statistical Area Code
        @cbsa_t: Core Based Statistical Area Code Title
        """
        if self.has_cbsa09:
            self.cbsa_dict[cbsa09] = [cbsa_t, 0, 0, 0, 0.00]
    
    def get_cbsa_t(self, cbsa09):
        """
        get the Core Based Statistical Area Code Title
        """
        return self.cbsa_dict[cbsa09][0]
    
    def get_tol_num_tract(self, cbsa09):
        """
        get the total number of a census tract
        """
        if self.has_cbsa09(cbsa09):
            return self.cbsa_dict[cbsa09][1]
    
    def get_tol_pop_00(self, cbsa09):
        """
        get total population in 2000
        """
        if self.has_cbsa09(cbsa09):
            return self.cbsa_dict[cbsa09][2]
    
    def get_tol_pop_10(self, cbsa09):
        """
        get total population in 2010
        """
        if not self.has_cbsa09(cbsa09): return
        return self.cbsa_dict[cbsa09][3]
    
    def add_tol_num_tract(self, cbsa09, n=1):
        """
        update the total number of a census tract
        """
        self.cbsa_dict[cbsa09][1] += n
    
    def add_tol_pop_00(self, cbsa09,n):
        """
        update total population in 2000
        """
        self.cbsa_dict[cbsa09][2] += n
    
    def add_tol_pop_10(self, cbsa09,n):
        """
        update total population in 2010
        """
        self.cbsa_dict[cbsa09][3] += n
    
    def get_cbsa_dict(self):
        """
        get the whole dict of CBSA
        """
        return self.cbsa_dict
    
    def join(self, other_cbsa):
        """
        merge 2 CBSA objects
        @other_cbsa: CBSA object
        """
        other_cbsa_dict = other_cbsa.get_cbsa_dict()
        for key in other_cbsa_dict.keys():
            if not self.has_cbsa09(key):
                self.new_cbsa_t(key,other_cbsa.get_cbsa_t(key))
            self.add_tol_num_tract(key,other_cbsa.get_tol_num_tract(key))
            self.add_tol_pop_00(key,other_cbsa.get_tol_pop_00(key))
            self.add_tol_pop_10(key,other_cbsa.get_tol_pop_10(key))
    
    def calc_growth_rate(self):
        """
        calculate the average population percent change for census tracts
        """
        for key, value in self.cbsa_dict.items():
            pop_change = float(value[3] - value[2])/ float(value[2])
            self.cbsa_dict[key][4] = round(pop_change,2)
            
    def save_report(self,file_name):
        """
        sort the result by cbsa09 and save it to report.csv
        """
        with open(file_name, "w") as f:
            sorted_cbsa09 = sorted(list(self.cbsa_dict.keys()))
            for cbsa09 in sorted_cbsa09:
                f.write('{}\n'.format(','.join([cbsa09] + ['"'+self.cbsa_dict[cbsa09][0]+'"'] + [str(v) for v in self.cbsa_dict[cbsa09][1:]])))
