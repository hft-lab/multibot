import traceback


class Shifts:

    def __init__(self):
        with open(f'rates.txt', 'r') as file:
            data = file.read().split('\n\n')

        self.data = data[-300000:]
        self.list_len = len(data)
        self.price_deviations = []

    def find_deviations(self):
        for record in self.data:
            prices = {}
            for exchange in record.split('\n'):
                if exchange == '':
                    continue
                name = exchange.split(' | ')[0]
                price = float(exchange.split(' | ')[1])
                prices.update({name: price})

            # avr_price = sum([x for x in prices.values()]) / len(prices.values())
            new_record = {}
            for name_1 in prices.keys():
                for name_2 in prices.keys():
                    if name_2 == name_1:
                        continue
                    if len(name_1) > 15 or len(name_2) > 15:
                        continue
                    deviation_value = (prices[name_1] - prices[name_2]) / prices[name_2]
                    # print(f'{exchange}: {deviation_value}')
                    new_record.update({name_1 + ' ' + name_2: deviation_value})
                    new_record.update({name_2 + ' ' + name_1: -deviation_value})
            self.price_deviations.append(new_record)

    def get_shifts(self):
        self.find_deviations()
        gathering_time = self.list_len / 240
        shifts = {}
        print(f"Result for {gathering_time} hours")
        all_shifts = [set(x) for x in self.price_deviations]
        last_list = set()
        for shift in all_shifts:
            last_list = last_list.union(shift)
        for exchange in last_list:
            try:
                list_of_deviations = [x.get(exchange) for x in self.price_deviations if x.get(exchange)]
                avg_deviation = sum(list_of_deviations) / len(list_of_deviations)
            except Exception:
                traceback.print_exc()
                avg_deviation = 0

            shifts.update({exchange: round(avg_deviation, 6)})

        print(f"SHIFTS: {shifts}")

        return shifts
