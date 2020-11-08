
import numpy as np
import pymc3 as pm
import pandas as pd
import pickle 

def statistics_from_trace_model(rt_trace):
    r_t = rt_trace
    mean = np.mean(r_t, axis=0)
    median = np.median(r_t, axis=0)
    hpd_90 = pm.stats.hpd(r_t, hdi_prob=.9)
    hpd_50 = pm.stats.hpd(r_t, hdi_prob=.5)

    df = pd.DataFrame(data=np.c_[mean, median, hpd_90, hpd_50],
                 columns=['mean', 'median', 'lower_90', 'upper_90', 'lower_50','upper_50'])

    return df   

def calculate_threshold(a, r):
    if a == 0 :
        return None
    return (1 - (np.log(r) / a))

def mov_th_mcmcm_model(mt, onset, poly_id, path_to_save_trace=None):

    with pm.Model() as Rt_mobility_model:            
        # Create the alpha and beta parameters
        # Assume a uninformed distribution
        beta  = pm.Uniform('beta', lower=-100, upper=100)
        Ro    = pm.Uniform('R0', lower=2, upper=5)

        # The effective reproductive number is given by:
        Rt              = pm.Deterministic('Rt', Ro*pm.math.exp(-beta*(1+mt[:-1].values)))
        serial_interval = pm.Gamma('serial_interval', alpha=6, beta=1.5)
        GAMMA           = 1 / serial_interval
        lam             = onset[:-1].values * pm.math.exp( GAMMA * (Rt- 1))
        observed        = onset.round().values[1:]

        # Likelihood
        cases = pm.Poisson('cases', mu=lam, observed=observed)

        with Rt_mobility_model:
            # Draw the specified number of samples
            N_SAMPLES = 10000
            # Using Metropolis Hastings Sampling
            step     = pm.Metropolis(vars=[ Rt_mobility_model.beta, Rt_mobility_model.R0 ], S = np.array([ (100+100)**2 , (5-2)**2 ]) )
            Rt_trace = pm.sample( N_SAMPLES, tune=1000, chains=10, step=step )

        BURN_IN = 2000
        rt_info = statistics_from_trace_model(Rt_trace.get_values(burn=BURN_IN,varname='Rt'))

        R0_dist   = Rt_trace.get_values(burn=BURN_IN, varname='R0')
        beta_dist = Rt_trace.get_values(burn=BURN_IN,varname='beta')
        mb_th     = calculate_threshold(beta_dist.mean(), R0_dist.mean())

        if path_to_save_trace:
            with open(path_to_save_trace, 'wb') as buff:
                pickle.dump({'model': Rt_mobility_model, 'trace': Rt_trace }, buff)

    return {'poly_id': poly_id, 'R0':R0_dist.mean(), 'beta':beta_dist.mean(), 'mob_th':mb_th }

