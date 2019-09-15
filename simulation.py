import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from math import sqrt


def GBM(S_0, t, mu, sigma, delta=1/252):
    drift = mu - (sigma**2)/2
    # normal distribution
    diffusion = sigma * np.random.normal(loc=0, scale=sqrt(delta), size=(t,))
    S = np.exp(drift * delta + diffusion)
    # Geometric Brownian Motion
    S = S_0*S.cumprod() # GBM
    return S


if __name__ == "__main__":
    # stock price daily drift (1%)
    mu = 1
    # stock price daily volatility
    sigma = 0.8
    # number of periods (dyas) to simulate
    T = 252*3
    # initial stock price
    S_0 = 100
    delta = 1 / T  # time increments

    x = pd.DataFrame()
    for i in range(1,13):
        np.random.seed(i)
        y = GBM(S_0, T, mu, sigma, delta)
        plt.plot(y)
    plt.title("GBM simulation different Seeds")
    plt.show()