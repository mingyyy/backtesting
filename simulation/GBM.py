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
    S = S_0*S.cumprod()
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
    # time increments
    delta = 1 / T

    x = pd.DataFrame()
    for i in range(1,10):
        np.random.seed(i)
        y = GBM(S_0, T, mu, sigma, delta)
        plt.plot(y)
    plt.title("GBM simulation with different Seeds")
    plt.show()