from abc import abstractmethod, ABCMeta

def get_varying_combinations(params_headers, params_values, ignored_keys):

    # Index keys
    params_idx = {}
    for key_idx, key in enumerate(params_headers):
        params_idx[key] = key_idx

    # Get varying keys
    varying_keys = [key for key in params_headers if key not in ignored_keys]
    varying_values = []
    for key in varying_keys[:]: # Slice to allow removal during iteration
        key_idx = params_idx[key]
        values = set([])
        for row in params_values:
            values.add(row[key_idx])
        if len(values) == 1:
            varying_keys.remove(key)
        else:
            varying_values.append(values)

    varying_keys_idxs = [params_headers.index(key) for key in varying_keys]

    varying_combinations = set([])
    for row in params_values:
        comb = tuple([row[idx] for idx in varying_keys_idxs])
        if not comb in  varying_combinations:
            varying_combinations.add(comb)

    return (varying_keys, varying_values, varying_combinations)


class FigureLines:

    def __init__(self, params_headers, params_values,
                 metrics_headers, metric_values,
                 x_var, y_var, fixed_params=None, ignored_keys=None):

        if not fixed_params:
            fixed_params = {}

        if not ignored_keys:
            ignored_keys = []

        # Check dimensions
        if len(params_values) != len(metric_values):
            print "Number of params combinations is different to number of results"

        # Check vars
        if not x_var in params_headers and not x_var in metrics_headers:
            print "x_var " + x_var + " not in params_headers"
        if not y_var in metrics_headers:
            print "y_var " + y_var + " not in metrics_headers"

        fixed_keys = []
        fixed_values = []
        for key, value in fixed_params.iteritems():
            fixed_keys.append(key)
            fixed_values.append(value)
            if key not in params_headers:
                print "fixed param " + key + " not in params_headers"

        self.fixed_keys = fixed_keys
        self.fixed_values = fixed_values
        self.ignored_keys = ignored_keys
        self.x_var = x_var
        self.y_var = y_var

        # Construct keys_dict
        params_idx = {}
        for key_idx, key in enumerate(params_headers):
            params_idx[key] = key_idx
        metrics_idx = {}
        for key_idx, key in enumerate(metrics_headers):
            metrics_idx[key] = key_idx

        # Remove fixed_keys, ignored_keys and x_var
        varying_keys = []
        for key in params_headers:
            if key not in fixed_keys and key not in ignored_keys and key != x_var:
                varying_keys.append(key)

        # Filter rows
        kept_idxs = []
        for row_idx, row in enumerate(params_values):

            # Filter out non-matching fixed_keys
            for key in fixed_keys:
                fixed_value = fixed_params[key]
                if row[params_idx[key]] != fixed_value:
                    break
            else:
                kept_idxs.append(row_idx)

        # Remove non_varying_keys
        varying_values = []
        non_varying_keys = []
        non_varying_values = []
        for key in varying_keys[:]: # Slice to allow removal during iteration
            key_idx = params_idx[key]
            values = set([])
            for row_idx in kept_idxs:
                values.add(params_values[row_idx][key_idx])
            if len(values) == 1:
                varying_keys.remove(key)
                non_varying_keys.append(key)
                non_varying_values.append(values.pop())
            else:
                varying_values.append(values)

        self.varying_keys = varying_keys
        self.varying_values = varying_values
        self.non_varying_keys = non_varying_keys
        self.non_varying_values = non_varying_values

        # Construct output data
        lines_params = []
        lines_values = []
        for row_idx in kept_idxs:
            var_params = []
            for key in varying_keys:
                var_params.append(params_values[row_idx][params_idx[key]])
            if x_var in params_headers:
                x_y = [params_values[row_idx][params_idx[x_var]],
                       metric_values[row_idx][metrics_idx[y_var]]]
            else:
                x_y = [metric_values[row_idx][metrics_idx[x_var]],
                       metric_values[row_idx][metrics_idx[y_var]]]

            if var_params in lines_params:
                idx = lines_params.index(var_params)
                lines_values[idx].append(x_y)
            else:
                lines_params.append(var_params)
                lines_values.append([x_y])

        self.lines_params = lines_params
        self.lines_values = lines_values

        # Sort line values by x_var
        for row in lines_values:
            row.sort()


    def __str__(self):

        out = ""

        assigs = []
        for idx, key in enumerate(self.fixed_keys):
            assigs.append(str(key) + " = " + str(self.fixed_values[idx]))
        out += "fixed_params: " + ", ".join(assigs) + "\n"

        assigs = []
        for idx, key in enumerate(self.varying_keys):
            assigs.append(str(key) + " = " + str(self.varying_values[idx]))
        out += "varying_params: " + ", ".join(assigs) + "\n"

        assigs = []
        for idx, key in enumerate(self.non_varying_keys):
            assigs.append(str(key) + " = " + str(self.non_varying_values[idx]))
        out += "non_varying_params: " + ", ".join(assigs) + "\n"
        out += "x_var: " + str(self.x_var) + "\n"
        out += "y_var: " + str(self.y_var) + "\n"
        out += "lines_params: " + str(self.lines_params) + "\n"
        out += "lines_values: " + str(self.lines_values)

        return out


class FigureGenerator(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def draw_figure(self, figure_lines, out_file, title=None):
        pass

if __name__ == "__main__":

    params_headers = ["b", "c", "d", "e"]
    params_values = [[1, 0, "v1", "w2"],
                     [2, 0, "v1", "w2"],
                     [3, 0, "v1", "w2"],
                     [4, 0, "v1", "w2"],
                     [1, 1, "v1", "w2"],
                     [2, 1, "v1", "w2"],
                     [3, 1, "v1", "w2"],
                     [1, 1, "v1", "w0"],
                     [2, 1, "v1", "w0"],
                     [3, 1, "v1", "w0"]]

    metrics_headers = ["m1", "m2"]
    metrics_values = [[1,  2],
                      [2,  4],
                      [3,  6],
                      [4,  8],
                      [1,  3],
                      [2,  6],
                      [3,  9],
                      [1,  3],
                      [2,  6],
                      [3,  9]]

    x_var = "b"
    y_var = "m1"

    fls = FigureLines(params_headers, params_values,
                            metrics_headers, metrics_values,
                            x_var, y_var, {"e": "w0"})

    print "fls",fls
    print "-------------"

    y_var = "m2"
    fls = FigureLines(params_headers, params_values,
                      metrics_headers, metrics_values,
                      x_var, y_var)

    print fls
