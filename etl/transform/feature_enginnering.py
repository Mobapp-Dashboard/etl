#!/usr/bin/env ipython
import math
from datetime import datetime

import numpy as np
import pandas as pd
from geopy.distance import geodesic

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class Features_Engineering:
    def __init__(self, trajectory_id, latitude, longitude, timestamp):
        self.trajectory_id = trajectory_id
        self.coordinates = [latitude, longitude]
        self.timestamp = timestamp

    def __call__(self, data):
        logger.info("Setting new features")
        data["hour"] = data[f"_{self.timestamp}"].dt.hour
        logger.info("Visiting old points")
        data = self._visit_old_point(data)
        logger.info("Add speed column")
        data = self._add_speed(data)
        logger.info("Add acc column")
        data = self._add_acceleration(data)
        logger.info("Add turn column / Discretize time")
        data = self._discretize_time(data)
        logger.info("Listo!")
        return data

    def _add_speed(self, data):
        data["speed"] = data.dist_old_point / data.time_old_point
        ## Se a divisão foi por zero (resulta NaN) é pq o gps pegou dois ponto
        ## com mesmo timestamp, assim atribui velocidade zero
        ## pq deslocamento zero tmb - Verificar se não distorce os dados!
        data["speed"].fillna(value=0, inplace=True)
        return data

    def _add_acceleration(self, data):
        data["acceleration"] = data.dist_old_point / (
            data.time_old_point * data.time_old_point
        )
        ## Se a divisão foi por zero (resulta NaN) é pq o gps pegou dois ponto
        ## com mesmo timestamp, assim atribui aceleração zero
        ## pq deslocamento zero tmb - Verificar se não distorce os dados!
        data["acceleration"].fillna(value=0, inplace=True)
        return data

    def _divide_to_map(self, data, func):
        data = data.sort_values(self.timestamp)
        data = [x for _, x in data.groupby(self.trajectory_id)]
        data = list(map(func, data))
        # concatena as trajetorias de volta em um DF unico
        return pd.concat(data)

    def _visit_old_point(self, data):
        data = self._divide_to_map(data, self._calc_deltas)
        data = self._divide_to_map(data, self._calc_bearing)
        return data

    @staticmethod
    def _delta_time(t1, t2):
        """
        Retorna diferença temporal em segundos
        Ou np.nan se a diferença temporal para o ponto anterior
        for superior a 5 minutos
        """
        t1 = pd.to_datetime(t1, unit="us")
        t2 = pd.to_datetime(t2, unit="us")
        time = pd.Timedelta(np.abs(t2 - t1))
        if time.seconds > 5 * 60:
            return np.nan
        else:
            return time.seconds

    def _calc_deltas(self, data_trajetoria):
        """
        Retorna o DF com as colunas de delta[tempo, distancia] preenchidas
        Depende do valor da linha anterior (temporalmente)
        Nas funções MAP são enviados os valores da linha presente e da anterior (shift(1))
        """
        data_trajetoria["dist_old_point"] = 0
        data_trajetoria["time_old_point"] = 0
        delta_d = list(
            map(
                lambda x, y: geodesic(x, y).meters,
                data_trajetoria[self.coordinates].values[1:],
                data_trajetoria[self.coordinates].shift(1).values[1:],
            )
        )

        delta_t = list(
            map(
                lambda x, y: self._delta_time(x, y),
                data_trajetoria[self.timestamp].values[1:],
                data_trajetoria[self.timestamp].shift(1).values[1:],
            )
        )
        data_trajetoria["dist_old_point"] = [0, *delta_d]
        data_trajetoria["time_old_point"] = [0, *delta_t]
        ##cumulative

        data_trajetoria["cum_dist"] = data_trajetoria["dist_old_point"].cumsum(axis=0)
        data_trajetoria["cum_time"] = data_trajetoria["time_old_point"].cumsum(axis=0)

        # first and last
        # position

        data_trajetoria["first_lat"] = data_trajetoria.iloc[0]["lat"]
        data_trajetoria["first_lng"] = data_trajetoria.iloc[0]["lng"]

        data_trajetoria["last_lat"] = data_trajetoria.iloc[-1]["lat"]
        data_trajetoria["last_lng"] = data_trajetoria.iloc[-1]["lng"]

        # timestamp
        data_trajetoria["first_instant"] = data_trajetoria.iloc[0]["_timestamp"]

        data_trajetoria["last_instant"] = data_trajetoria.iloc[-1]["_timestamp"]

        ##

        data_trajetoria = self._remove_deltatime_gt_5min(data_trajetoria)

        return data_trajetoria

    def _calc_bearing(self, data_trajetoria):
        bearing = list(
            map(
                lambda x, y: self._bearing(x, y),
                data_trajetoria[self.coordinates].shift(1).values[1:],
                data_trajetoria[self.coordinates].values[1:],
            )
        )
        data_trajetoria["bearing"] = [*bearing, np.nan]
        return data_trajetoria

    def _bearing(self, point1, point2):
        lat1 = math.radians(point1[0])

        lat2 = math.radians(point2[0])

        y = math.sin(math.radians(point2[1] - point1[1])) * math.cos(lat2)

        x = math.cos(lat1) * math.sin(lat2) - (
            math.sin(lat1)
            * math.cos(lat2)
            * math.cos(math.radians(point2[1] - point1[1]))
        )

        deg = math.degrees(math.atan2(x, y))
        return (deg + 360) % 360

    def _remove_deltatime_gt_5min(self, trajetoria):
        ## Só pega o primeiro gap de 5 minutos
        gt5min2oldpoint = trajetoria[trajetoria["time_old_point"].isna()]
        if len(gt5min2oldpoint) == 0:
            return trajetoria
        ts = gt5min2oldpoint[self.timestamp].values[0]
        return None

    @staticmethod
    def _discretize_time(data):
        data["turn"] = ""
        conditions = [
            (~(data["hour"] < 5)),
            (~((data["hour"] >= 5) & (data["hour"] < 12))),
            (~((data["hour"] >= 12) & (data["hour"] < 18))),
            (~(data["hour"] >= 18)),
        ]
        data["turn"].where(conditions[0], "DAWN", inplace=True)
        data["turn"].where(conditions[1], "MORNING", inplace=True)
        data["turn"].where(conditions[2], "AFTERNOON", inplace=True)
        data["turn"].where(conditions[3], "EVENING", inplace=True)
        return data
