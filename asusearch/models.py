import torch
from torch import nn


class CorrectorRNN(nn.Module):
    def __init__(self, vocab_size, embed_size, hidden_dim, n_layers):
        super(CorrectorRNN, self).__init__()
        self.embed = nn.Embedding(num_embeddings=vocab_size, embedding_dim=embed_size)
        self.rnn = nn.RNN(input_size=embed_size, hidden_size=hidden_dim, num_layers=n_layers, batch_first=True)
        self.linear = nn.Linear(hidden_dim, 4)

    def forward(self, x):
        embeds = self.embed(x)
        output, _ = self.rnn(embeds)
        output = self.linear(output)
        return output

class Corrector:
    def __init__(self):
        self.LEN_SENT = 50
        self.CIRILL_TO_LATIN = {'й': 'q', 'ц': 'w', 'у': 'e', 'к': 'r', 'е': 't', 'н': 'y', 'г': 'u', 'ш': 'i',
                                'щ': 'o',
                                'з': 'p', 'х': '[', 'ъ': ']', 'ф': 'a', 'ы': 's', 'в': 'd', 'а': 'f', 'п': 'g',
                                'р': 'h',
                                'о': 'j', 'л': 'k', 'д': 'l', 'ж': ';', 'э': '\'', 'я': 'z', 'ч': 'x', 'с': 'c',
                                'м': 'v',
                                'и': 'b', 'т': 'n', 'ь': 'm', 'б': ',', 'ю': '.', 'ё': '`'}
        self.LATIN_TO_CIRILL = {'q': 'й', 'w': 'ц', 'e': 'у', 'r': 'к', 't': 'е', 'y': 'н', 'u': 'г', 'i': 'ш',
                                'o': 'щ',
                                'p': 'з', '[': 'х', ']': 'ъ', 'a': 'ф', 's': 'ы', 'd': 'в', 'f': 'а', 'g': 'п',
                                'h': 'р',
                                'j': 'о', 'k': 'л', 'l': 'д', ';': 'ж', "'": 'э', 'z': 'я', 'x': 'ч', 'c': 'с',
                                'v': 'м',
                                'b': 'и', 'n': 'т', 'm': 'ь', ',': 'б', '.': 'ю', '`': 'ё'}

        self.ALPHABET = {}
        symbols = list(self.LATIN_TO_CIRILL.keys()) + list(self.CIRILL_TO_LATIN.keys())
        for i, s in enumerate(symbols):
            self.ALPHABET[s] = i

        self.model = CorrectorRNN(len(self.ALPHABET) + 1, 16, 128, 1)
        self.model.load_state_dict(torch.load('asusearch/models/corrector_RNN.pth'))

    def str_to_vector(self, s: str):
        vector = []
        other_idx = len(self.ALPHABET)
        for sym in s:
            if self.ALPHABET.get(sym):
                vector.append(self.ALPHABET[sym])
            else:
                vector.append(other_idx)

        return vector

    def correct_keyboard_layout(self, s: str):
        """
        текст на русском - 0
        текст на английском - 1
        текст на англ. в кирилл. раскладке - 2
        текст на рус. в латинской раскладке - 3
        :param self:
        :param s:
        :return:
        """
        s = s.lower()
        vector = self.str_to_vector(s)

        if len(vector) < self.LEN_SENT:
            vector += [66] * (self.LEN_SENT - len(vector))
        else:
            vector = vector[:self.LEN_SENT]

        y = self.model(torch.tensor(vector))
        out = y[0].argmax()

        result = ''
        if out == 2:
            for sym in s:
                if self.CIRILL_TO_LATIN.get(sym):
                    result += self.CIRILL_TO_LATIN[sym]
        elif out == 3:
            for sym in s:
                if self.LATIN_TO_CIRILL.get(sym):
                    result += self.LATIN_TO_CIRILL[sym]
        else:
            result = s

        return result
